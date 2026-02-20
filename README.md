# Mini MLOps Pipeline Task

This repository contains a deterministic batch-style MLOps pipeline that:
- loads configuration from YAML,
- ingests OHLCV CSV data,
- computes a rolling mean over `close`,
- generates binary signals,
- outputs machine-readable JSON metrics,
- logs all processing steps,
- runs locally and in Docker.

## Setup Instructions
```bash
# Install dependencies
pip install -r requirements.txt
```

## Local Execution Instructions
```bash
# Run locally
python run.py --input data.csv --config config.yaml \
    --output metrics.json --log-file run.log
```

## Docker Instructions
```bash
# Build the Docker image
docker build -t mlops-task .

# Run the container
docker run --rm mlops-task
```

## Expected Output (`metrics.json`)
```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4966,
  "latency_ms": 79,
  "seed": 42,
  "status": "success"
}
```

## Dependencies
Dependencies:
- pandas
- numpy
- pyyaml
