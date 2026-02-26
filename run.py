import argparse
import csv
import json
import logging
import random
import sys
import time
from pathlib import Path
from typing import Dict, List

REQUIRED_CONFIG_FIELDS = ("seed", "window", "version")


def setup_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("mlops_task")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def write_metrics(output_path: Path, metrics: Dict) -> None:
    output_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")


def parse_simple_yaml(yaml_text: str) -> Dict:
    config: Dict[str, str] = {}
    for raw_line in yaml_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            raise ValueError(f"Invalid YAML line: {raw_line}")
        key, value = line.split(":", 1)
        config[key.strip()] = value.strip()
    return config


def load_and_validate_config(config_path: Path) -> Dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw_config = parse_simple_yaml(config_path.read_text(encoding="utf-8"))

    missing = [field for field in REQUIRED_CONFIG_FIELDS if field not in raw_config]
    if missing:
        raise ValueError(f"Config missing required field(s): {', '.join(missing)}")

    try:
        seed = int(raw_config["seed"])
    except ValueError as exc:
        raise ValueError("Invalid config: seed must be an integer") from exc

    try:
        window = int(raw_config["window"])
    except ValueError as exc:
        raise ValueError("Invalid config: window must be an integer") from exc

    version = raw_config["version"].strip('"').strip("'")

    if window <= 0:
        raise ValueError("Invalid config: window must be a positive integer")
    if not version:
        raise ValueError("Invalid config: version must be a non-empty string")

    return {"seed": seed, "window": window, "version": version}


def load_and_validate_data(input_path: Path) -> List[float]:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if input_path.stat().st_size == 0:
        raise ValueError(f"Input file is empty: {input_path}")

    try:
        with input_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError("Invalid CSV format: missing header row")
            if "close" not in reader.fieldnames:
                raise ValueError("Missing required column: close")

            closes: List[float] = []
            for idx, row in enumerate(reader, start=1):
                close_value = row.get("close", "")
                if close_value is None or str(close_value).strip() == "":
                    raise ValueError(f"Invalid CSV format: empty close at row {idx}")
                try:
                    closes.append(float(close_value))
                except ValueError as exc:
                    raise ValueError(f"Invalid CSV format: non-numeric close at row {idx}") from exc
    except csv.Error as exc:
        raise ValueError(f"Invalid CSV format: {exc}") from exc

    if not closes:
        raise ValueError("Input CSV contains no rows")

    return closes


def compute_signal_rate(closes: List[float], window: int) -> float:
    signals: List[int] = []
    rolling_buffer: List[float] = []

    for close in closes:
        rolling_buffer.append(close)
        if len(rolling_buffer) > window:
            rolling_buffer.pop(0)

        if len(rolling_buffer) < window:
            signals.append(0)
            continue

        rolling_mean = sum(rolling_buffer) / window
        signals.append(1 if close > rolling_mean else 0)

    return sum(signals) / len(signals)


def run_job(input_path: Path, config_path: Path, output_path: Path, log_file: Path) -> int:
    logger = setup_logger(log_file)
    start = time.perf_counter()
    version_for_error = "v1"

    logger.info("Job started")

    try:
        config = load_and_validate_config(config_path)
        version_for_error = config["version"]

        random.seed(config["seed"])
        logger.info(
            "Config loaded and validated | seed=%s window=%s version=%s",
            config["seed"],
            config["window"],
            config["version"],
        )

        closes = load_and_validate_data(input_path)
        logger.info("Rows loaded: %s", len(closes))

        logger.info("Computing rolling mean on close with window=%s", config["window"])
        logger.info("Generating binary signal (first window-1 rows assigned signal=0)")
        signal_rate = compute_signal_rate(closes, config["window"])

        latency_ms = int((time.perf_counter() - start) * 1000)

        metrics = {
            "version": config["version"],
            "rows_processed": int(len(closes)),
            "metric": "signal_rate",
            "value": round(float(signal_rate), 4),
            "latency_ms": latency_ms,
            "seed": config["seed"],
            "status": "success",
        }
        write_metrics(output_path, metrics)

        logger.info("Metrics summary: %s", metrics)
        logger.info("Job ended with status=success")

        print(json.dumps(metrics, indent=2))
        return 0

    except Exception as exc:
        error_metrics = {
            "version": version_for_error,
            "status": "error",
            "error_message": str(exc),
        }
        write_metrics(output_path, error_metrics)

        logger.exception("Job failed")
        logger.info("Job ended with status=error")

        print(json.dumps(error_metrics, indent=2))
        return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Minimal MLOps-style batch job")
    parser.add_argument("--input", required=False, default="data.csv", help="Path to input CSV")
    parser.add_argument("--config", required=False, default="config.yaml", help="Path to YAML config")
    parser.add_argument("--output", required=False, default="metrics.json", help="Path to output metrics JSON")
    parser.add_argument("--log-file", required=False, default="run.log", help="Path to log file")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    exit_code = run_job(
        input_path=Path(args.input),
        config_path=Path(args.config),
        output_path=Path(args.output),
        log_file=Path(args.log_file),
    )
    raise SystemExit(exit_code)
