import logging

import pytest

from postgres_connector import pool as pool_module
from postgres_connector.pool import ConnectionPool


def _pool(echo):
    return ConnectionPool(host="127.0.0.1", user="u", password="p", database="d", echo=echo)


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@pytest.fixture
def fresh_echo():
    """Reset the echo logger to a never-configured state (pytest adds its own handlers)."""
    logger = logging.getLogger("postgres_connector.echo")
    saved = (list(logger.handlers), logger.level, getattr(pool_module, "_echo_handler_installed", False))

    def reset():
        for h in list(logger.handlers):
            logger.removeHandler(h)
        pool_module._echo_handler_installed = False

    yield logger, reset
    for h in list(logger.handlers):
        logger.removeHandler(h)
    for h in saved[0]:
        logger.addHandler(h)
    logger.setLevel(saved[1])
    pool_module._echo_handler_installed = saved[2]


def test_echo_prints_sql_to_stdout_without_ansi(capsys, fresh_echo):
    logger, reset = fresh_echo
    reset()
    pool = _pool(True)
    pool.log_query("SELECT  1\n FROM tags", [1])
    out = capsys.readouterr().out
    assert "SELECT 1 FROM tags" in out
    assert "[1]" in out
    assert "\x1b" not in out


def test_record_messages_have_no_ansi(fresh_echo):
    logger, reset = fresh_echo
    reset()
    pool = _pool(True)
    cap = _Capture()
    logger.addHandler(cap)
    pool.log_query("SELECT 1", [1])
    assert any("SELECT 1" in m for m in cap.messages)
    assert all("\x1b" not in m for m in cap.messages)


def test_echo_false_prints_nothing(capsys):
    _pool(False).log_query("SELECT 1", [1])
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


def test_multiple_echo_pools_add_one_handler(fresh_echo):
    logger, reset = fresh_echo
    reset()
    _pool(True).log_query("SELECT 1", None)
    _pool(True).log_query("SELECT 2", None)
    assert sum(type(h).__name__ == "_StdoutHandler" for h in logger.handlers) == 1


def test_existing_handler_is_respected(capsys, fresh_echo):
    logger, reset = fresh_echo
    reset()
    cap = _Capture()
    logger.addHandler(cap)
    _pool(True).log_query("SELECT 1", None)
    assert cap.messages
    assert capsys.readouterr().out == ""
    assert not any(type(h).__name__ == "_StdoutHandler" for h in logger.handlers)


def test_handler_attached_after_construction_replaces_default(capsys, fresh_echo):
    logger, reset = fresh_echo
    reset()
    pool = _pool(True)
    cap = _Capture()
    logger.addHandler(cap)
    pool.log_query("SELECT 1", None)
    assert cap.messages
    assert capsys.readouterr().out == ""
