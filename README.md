# T0 - Minimal MLOps Batch Job

This repository contains a minimal Python batch pipeline that demonstrates deterministic processing, logging/metrics observability, and Dockerized execution.

## What it does
- Loads runtime config from `config.yaml`
- Reads `data.csv` and validates required input
- Computes rolling mean on `close` using configured `window`
- Generates binary signal:
  - `signal = 1` if `close > rolling_mean`
  - else `0`
- Writes structured metrics JSON to `metrics.json`
- Writes detailed execution logs to `run.log`

### Determinism
The run is deterministic via config-based seed:
- `seed` from config is applied via Python's deterministic RNG seeding (`random.seed(seed)`)

## Local run
Install dependencies:

```bash
pip install -r requirements.txt
```

Run:

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

## Docker
Build image:

```bash
docker build -t mlops-task .
```

Run container:

```bash
docker run --rm mlops-task
```

The container includes `data.csv` and `config.yaml`, writes `metrics.json` and `run.log`, prints final metrics JSON to stdout, and exits `0` on success / non-zero on failure.

## Example `metrics.json`

```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4896,
  "latency_ms": 21,
  "seed": 42,
  "status": "success"
}
```
