import datetime

import pytest

from dbcsv.engine.relational.datatype import (
    DBTypeObject,
)

# --- Successful conversions ---


def test_string_conversion():
    assert DBTypeObject.convert_datatype("'hello'", "VARCHAR") == "hello"
    assert DBTypeObject.convert_datatype("plain", "VARCHAR") == "plain"


def test_integer_conversion():
    assert DBTypeObject.convert_datatype("42", "INTEGER") == 42


def test_float_conversion():
    assert DBTypeObject.convert_datatype("3.14", "FLOAT") == 3.14


def test_boolean_conversion():
    assert DBTypeObject.convert_datatype("true", "BOOLEAN") is True
    assert DBTypeObject.convert_datatype("FALSE", "BOOL") is False


def test_date_conversion():
    assert DBTypeObject.convert_datatype("2024-04-01", "DATE") == datetime.date(
        2024, 4, 1
    )


def test_datetime_conversion():
    assert DBTypeObject.convert_datatype(
        "2024-04-01 12:30:00", "DATETIME"
    ) == datetime.datetime(2024, 4, 1, 12, 30, 0)


def test_null_conversion():
    assert DBTypeObject.convert_datatype("null", "NULL") is None


# --- Error cases ---


def test_string_given_to_integer_should_fail():
    with pytest.raises(ValueError, match="Invalid integer format: 'abc'"):
        DBTypeObject.convert_datatype("'abc'", "INTEGER")


def test_invalid_boolean():
    with pytest.raises(ValueError, match="Invalid boolean format: maybe"):
        DBTypeObject.convert_datatype("maybe", "BOOLEAN")


def test_invalid_date():
    with pytest.raises(ValueError, match="Invalid date format.*"):
        DBTypeObject.convert_datatype("20240401", "DATE")


def test_invalid_datetime():
    with pytest.raises(ValueError, match="Invalid datetime format.*"):
        DBTypeObject.convert_datatype("2024/04/01 12:30", "DATETIME")


def test_string_given_to_non_string_type_should_raise():
    with pytest.raises(ValueError, match="is a string, not a int"):
        DBTypeObject.convert_datatype("'123'", "INT")


# --- convert_rowtype ---


def test_convert_rowtype():
    row = {"id": "42", "name": "'Alice'", "active": "true"}
    types = ["INT", "VARCHAR", "BOOLEAN"]
    converted = DBTypeObject.convert_rowtype(row, types)
    assert converted == {"id": 42, "name": "Alice", "active": True}


def test_convert_rowtype_length_mismatch():
    row = {"id": "1", "name": "Bob"}
    types = ["INT"]  # mismatch
    with pytest.raises(
        ValueError, match="Row length 2 does not match column types length 1"
    ):
        DBTypeObject.convert_rowtype(row, types)
