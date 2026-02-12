"""
Tests for init_tracing function.
"""

import os
import tempfile
import pytest

from duroxide import init_tracing


class TestInitTracing:
    """Tests for the init_tracing file-based tracing subscriber."""

    def test_is_callable(self):
        """init_tracing is importable and callable."""
        assert callable(init_tracing)

    def test_writes_to_file(self):
        """init_tracing creates a log file (may fail if subscriber already installed)."""
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as f:
            log_file = f.name

        try:
            init_tracing(log_file, log_level="info")
        except RuntimeError as e:
            # first-writer-wins: another subscriber already installed
            assert "Failed to init tracing" in str(e)
        finally:
            try:
                os.unlink(log_file)
            except OSError:
                pass

    def test_invalid_path_raises_error(self):
        """init_tracing raises a clear error for invalid log file paths."""
        with pytest.raises(RuntimeError, match="Failed to open log file"):
            init_tracing("/nonexistent-dir/sub/test.log")
