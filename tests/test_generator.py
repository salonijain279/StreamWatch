from pathlib import Path

import pytest

from src.generate_review_batches import chunked, write_batches


def test_chunked_preserves_every_record() -> None:
    assert chunked(["a", "b", "c", "d", "e"], 2) == [["a", "b"], ["c", "d"], ["e"]]


def test_chunked_rejects_invalid_batch_size() -> None:
    with pytest.raises(ValueError):
        chunked(["a"], 0)


def test_writer_uses_final_batch_names(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    output = tmp_path / "stream"
    source.write_text("one\ntwo\nthree\n")
    assert write_batches(source, output, batch_size=2, delay=0) == 3
    assert [path.name for path in sorted(output.iterdir())] == ["batch_0001.txt", "batch_0002.txt"]

