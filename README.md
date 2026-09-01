# Real-Time Analytics with Spark Structured Streaming

This repository contains two portable Spark Structured Streaming pipelines: a text-signal monitor and a time-windowed IoT event monitor. Both implementations make ingestion, state management, checkpointing, and recovery behavior explicit.

## What I built

### Review signal monitor

The pipeline simulates incoming customer-review files, tokenizes each micro-batch, and maintains continuously updated word counts. Atomic file writes prevent Spark from reading a partial batch.

### IoT event monitor

The pipeline processes timestamped JSON events, applies a watermark, and calculates open/close event counts in configurable time windows without retaining an unbounded state table.

## Architecture

```text
source records -> atomic micro-batches -> Structured Streaming -> watermark/window -> console or Parquet sink
```

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python src/generate_review_batches.py \
  --source data/reviews.txt --output runtime/reviews --batch-size 3

spark-submit src/word_count_stream.py \
  --input runtime/reviews --checkpoint runtime/checkpoints/words

spark-submit src/iot_window_metrics.py \
  --input data/iot_events --checkpoint runtime/checkpoints/iot --trigger-once
```

## Engineering choices

- Static parsing logic is defined separately from streaming source/sink configuration.
- Checkpoints are explicit and never committed.
- Event-time aggregation uses watermarks to bound late-data state.
- Sample fixtures are synthetic and intentionally small; production data belongs in cloud/object storage.
