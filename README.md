# Real-Time Analytics with Spark Structured Streaming

A portable streaming casebook that moves from controlled micro-batch generation to two real-time analytics patterns: text signals and time-windowed IoT activity.

## Streaming projects

### Review signal monitor

Simulates incoming customer-review files, tokenizes each micro-batch, and maintains continuously updated word counts. The generator writes files atomically so Spark never reads a partial batch.

### IoT event monitor

Consumes timestamped JSON events, applies a watermark, and calculates open/close event counts in configurable time windows. The output supports operational monitoring without retaining an unbounded state table.

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

## Origin

Rebuilt from MSBA Spark Streaming labs as a portfolio-ready implementation. Course solution exports are not included.
