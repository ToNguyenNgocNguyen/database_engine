# test_dbapi2.py

import pytest

import dbcsv
from dbcsv import dbapi2
from dbcsv.dbapi2.exceptions import AuthenticationError, NotSupportedError

# Mock test values — replace with appropriate mock or test server setup
VALID_DSN = "http://localhost:80/schema1"
VALID_USER = "johndoe"
VALID_PASSWORD = "secret"
INVALID_PASSWORD = "wrong_password"


def test_connect_success():
    """Test successful connection."""
    conn = dbcsv.connect(dsn=VALID_DSN, user=VALID_USER, password=VALID_PASSWORD)
    assert conn is not None
    conn.close()


def test_connect_authentication_error():
    """Test connection failure with invalid credentials."""
    with pytest.raises(AuthenticationError):
        dbcsv.connect(dsn=VALID_DSN, user=VALID_USER, password=INVALID_PASSWORD)


def test_cursor_execute_and_fetch():
    """Test cursor creation, execution, and fetch."""
    conn = dbcsv.connect(dsn=VALID_DSN, user=VALID_USER, password=VALID_PASSWORD)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table1")
    rows = cursor.fetchall()
    assert isinstance(rows, list)
    cursor.close()
    conn.close()


def test_commit_raises_not_supported():
    """Test that commit raises NotSupportedError."""
    conn = dbcsv.connect(dsn=VALID_DSN, user=VALID_USER, password=VALID_PASSWORD)
    with pytest.raises(NotSupportedError):
        conn.commit()
    conn.close()


def test_rollback_raises_not_supported():
    """Test that rollback raises NotSupportedError."""
    conn = dbcsv.connect(dsn=VALID_DSN, user=VALID_USER, password=VALID_PASSWORD)
    with pytest.raises(NotSupportedError):
        conn.rollback()
    conn.close()
