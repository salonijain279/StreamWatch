# Real-Time Analytics with Spark Structured Streaming

I rebuilt two Big Data streaming exercises as portable projects: a text-signal monitor and a time-windowed IoT event monitor. My goal was to make the streaming mechanics visible—how data arrives, how state is bounded, and how a job can recover safely.

## What I built

### Review signal monitor

I simulated incoming customer-review files, tokenized each micro-batch, and maintained continuously updated word counts. I wrote files atomically so Spark never reads a partial batch.

### IoT event monitor

I processed timestamped JSON events, applied a watermark, and calculated open/close event counts in configurable time windows. This let me practice operational monitoring without retaining an unbounded state table.

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
