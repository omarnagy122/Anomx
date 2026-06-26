"""Backward-compatible processing entrypoint.

This wrapper keeps the historical file name but now runs the reviewed incremental
batch path: it reads only raw rows that arrived after the last successful
checkpoint, adds a small context window for rolling features, and writes
prediction outputs without touching the real-time consumer.
"""
from prediction.prediction_pipeline import main

if __name__ == "__main__":
    main()
