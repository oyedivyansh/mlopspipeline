import argparse
import csv
import json
import logging
import os
import random
import sys
import time
from collections import deque
from pathlib import Path

REQUIRED_CONFIG_FIELDS = {"seed", "window", "version"}
REQUIRED_COLUMNS = {"close"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mini MLOps batch pipeline")
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--config", required=True, help="YAML config file path")
    parser.add_argument("--output", required=True, help="Metrics output JSON path")
    parser.add_argument("--log-file", required=True, help="Log file path")
    return parser.parse_args()


def setup_logging(log_path: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )


def parse_simple_yaml(path: str) -> dict:
    data = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                raise ValueError("Invalid YAML configuration: expected key: value lines")
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            elif value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
                value = int(value)
            data[key] = value
    return data


def load_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    config = parse_simple_yaml(config_path)
    if not isinstance(config, dict):
        raise ValueError("Invalid configuration file structure: expected a YAML mapping")

    missing = REQUIRED_CONFIG_FIELDS - set(config.keys())
    if missing:
        raise ValueError(f"Invalid configuration file structure: missing fields {sorted(missing)}")

    seed = config["seed"]
    window = config["window"]
    version = config["version"]

    if not isinstance(seed, int):
        raise ValueError("Invalid configuration file structure: 'seed' must be an integer")
    if not isinstance(window, int) or window <= 0:
        raise ValueError("Invalid configuration file structure: 'window' must be a positive integer")
    if not isinstance(version, str) or not version.strip():
        raise ValueError("Invalid configuration file structure: 'version' must be a non-empty string")

    return {"seed": seed, "window": window, "version": version}


def load_rows(input_path: str) -> tuple[list[dict], list[str]]:
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if not os.access(input_path, os.R_OK):
        raise PermissionError(f"Input file is not readable: {input_path}")

    try:
        with open(input_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError("Invalid CSV file format: missing header row")
            rows = list(reader)
            headers = [h.strip() for h in reader.fieldnames]
    except UnicodeDecodeError as exc:
        raise ValueError(f"Invalid CSV file format: {exc}") from exc
    except csv.Error as exc:
        raise ValueError(f"Invalid CSV file format: {exc}") from exc

    if not rows:
        raise ValueError("Empty input file")

    missing_columns = REQUIRED_COLUMNS - set(headers)
    if missing_columns:
        raise ValueError(f"Missing required columns in dataset: {sorted(missing_columns)}")

    return rows, headers


def compute_signal_rate(rows: list[dict], window: int) -> float:
    rolling_values: deque[float] = deque(maxlen=window)
    signal_sum = 0

    for row in rows:
        try:
            close_val = float(row["close"])
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid CSV file format: non-numeric 'close' value encountered") from exc

        rolling_values.append(close_val)
        rolling_mean = sum(rolling_values) / len(rolling_values)
        signal = 1 if close_val > rolling_mean else 0
        signal_sum += signal

    return signal_sum / len(rows)


def write_json(path: str, payload: dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)


def main() -> int:
    args = parse_args()
    setup_logging(args.log_file)
    start_time = time.perf_counter()
    default_version = "v1"

    logging.info("Job started")

    try:
        config = load_config(args.config)
        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        random.seed(seed)
        logging.info("Config loaded: seed=%s, window=%s, version=%s", seed, window, version)
        logging.info("Configuration verified")

        rows, _ = load_rows(args.input)
        rows_processed = len(rows)
        logging.info("Data loaded: %s rows", rows_processed)

        logging.info("Rolling mean calculated with window=%s", window)
        signal_rate = compute_signal_rate(rows, window)
        logging.info("Signals generated")

        latency_ms = int((time.perf_counter() - start_time) * 1000)
        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success",
        }

        write_json(args.output, metrics)
        logging.info("Metrics: signal_rate=%.4f, rows_processed=%s", metrics["value"], rows_processed)
        logging.info("Job completed successfully in %sms", latency_ms)

        print(json.dumps(metrics, indent=2))
        return 0
    except Exception as exc:  # noqa: BLE001
        error_payload = {
            "version": default_version,
            "status": "error",
            "error_message": str(exc),
        }
        logging.error("Job failed: %s", exc, exc_info=True)

        try:
            if os.path.exists(args.config):
                error_payload["version"] = load_config(args.config).get("version", default_version)
        except Exception:
            pass

        write_json(args.output, error_payload)
        print(json.dumps(error_payload, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
