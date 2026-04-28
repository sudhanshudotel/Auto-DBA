"""Structured JSON logging with per-request correlation IDs.

The MCP server is invoked via stdio, so logs go to stderr — this module configures
a single JSON formatter on the root logger. Tool wrappers use `request_context()`
to attach a `tool` and `request_id` to every log record produced inside the block.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Optional

_request_id: ContextVar[Optional[str]] = ContextVar("auto_dba_request_id", default=None)
_tool_name: ContextVar[Optional[str]] = ContextVar("auto_dba_tool_name", default=None)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        rid = _request_id.get()
        if rid:
            payload["request_id"] = rid
        tool = _tool_name.get()
        if tool:
            payload["tool"] = tool
        return json.dumps(payload, default=str)


_configured = False


def configure() -> None:
    """Idempotently install the JSON formatter on stderr.

    Honors AUTO_DBA_LOG_LEVEL (default: INFO).
    """
    global _configured
    if _configured:
        return
    level = os.getenv("AUTO_DBA_LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    _configured = True


@contextmanager
def request_context(tool: str) -> Iterator[str]:
    """Attach a fresh request_id (and the tool name) to every log record in the block."""
    rid = uuid.uuid4().hex[:12]
    rid_token = _request_id.set(rid)
    tool_token = _tool_name.set(tool)
    try:
        yield rid
    finally:
        _request_id.reset(rid_token)
        _tool_name.reset(tool_token)
