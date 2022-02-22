#!/usr/bin/env python

import argparse
import datetime
import json
import logging
import os
import re
import time
import unicodedata

import httpx
import psycopg

logging.basicConfig(
    format="%(asctime)s %(message)s",
    level=logging.INFO,
)


DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")

INVALID_IN_NAME_RE = re.compile("[^a-z0-9_]")

DELAY = 1  # before HTTP requests when reading databases, for throttling

PAGE_SIZE = 64  # lower than the default of 100 to prevent timeouts


def get_database(database_id, token):
    """Get properties of a Notion database."""
    t0 = time.perf_counter()
    database = httpx.get(
        f"https://api.notion.com/v1/databases/{database_id}",
        headers={
            "Authorization": f"Bearer {token}",
            "Notion-Version": "2021-08-16",
        },
    ).json()
    t1 = time.perf_counter()
    logging.info("Fetched Notion database %s in %.1f seconds", database_id, t1 - t0)
    return database


def iter_database(database_id, token):
    """Iterate over the pages of a Notion database."""
    has_more = True
    query = {
        "sorts": [{"timestamp": "created_time", "direction": "descending"}],
        "page_size": PAGE_SIZE,
    }
    while has_more:
        time.sleep(DELAY)
        t0 = time.perf_counter()
        data = httpx.post(
            f"https://api.notion.com/v1/databases/{database_id}/query",
            headers={
                "Authorization": f"Bearer {token}",
                "Notion-Version": "2021-08-16",
            },
            json=query,
            timeout=120,
        ).json()
        t1 = time.perf_counter()
        if data["object"] == "error":
            raise Exception(f"HTTP {data['status']}: {data['message']}")
        assert data["object"] == "list"
        logging.info(
            "Fetched %d Notion pages in %.1f seconds", len(data["results"]), t1 - t0
        )
        has_more = data["has_more"]
        query["start_cursor"] = data["next_cursor"]
        yield from data["results"]


def get_value(property):
    """Convert a Notion property value to a Python value."""

    type_ = property["type"]

    if type_ == "title":
        # Optional[str]
        return "".join(t["plain_text"] for t in property["title"]) or None

    # Basic properties

    elif type_ == "rich_text":
        # Optional[str]
        return "".join(t["plain_text"] for t in property["rich_text"]) or None

    elif type_ == "number":
        # Optional[Number]
        return property["number"]

    elif type_ == "select":
        # Optional[str]
        if property["select"] is None:
            return None
        return property["select"]["name"]

    elif type_ == "multi_select":
        # List[str]
        return [ms["name"] for ms in property["multi_select"]]

    elif type_ == "date":
        # Tuple[Optional[str], Optional[str]] - start and end date or datetime
        if property["date"] is None:
            return None, None
        # "The public API will always return the time_zone field as null when
        # rendering dates and time zone will be displayed as a UTC offset in
        # the start and end date fields."
        assert property["date"]["time_zone"] is None
        return property["date"]["start"], property["date"]["end"]

    elif type_ == "people":
        # List[str] - UUID of person
        return [p["id"] for p in property["people"]]

    elif type_ == "files":
        # List[str] - URL of the file
        files = []
        for f in property["files"]:
            url = f["file"]["url"]
            # Remove authentication information from files uploaded to Notion;
            # it is too short lived to be worth storing in a database.
            if "/secure.notion-static.com/" in url:
                url = url.partition("?")[0]
            files.append(url)
        return files

    elif type_ == "checkbox":
        # bool
        return property["checkbox"]

    elif type_ == "url":
        # Optional[str]
        return property["url"]

    elif type_ == "email":
        # Optional[str]
        return property["email"]

    elif type_ == "phone_number":
        # Optional[str]
        return property["phone_number"]

    # Advanced properties

    elif type_ == "formula":
        formula = property["formula"]
        subtype = formula["type"]
        if subtype == "date":
            # Optional[str] - start date or datetime
            if formula["date"] is None:
                return ("date", None)
            assert formula["date"]["time_zone"] is None
            assert formula["date"]["end"] is None
            return ("date", formula["date"]["start"])
        elif subtype == "number":
            # Optional[Number]
            return ("number", formula["number"])
        elif subtype == "string":
            # str
            return ("string", formula["string"])
        raise NotImplementedError(f"unsupported formula: {json.dumps(formula)}")

    elif type_ == "relation":
        # List[str] - UUID of related object
        return [r["id"] for r in property["relation"]]

    elif type_ == "rollup":
        rollup = property["rollup"]
        subtype = rollup["type"]
        if subtype == "number":
            # Optional[Number]
            return ("number", rollup["number"])
        raise NotImplementedError(f"unsupported rollup: {json.dumps(rollup)}")

    elif type_ == "created_time":
        return property["created_time"]

    elif type_ == "created_by":
        return property["created_by"]["name"]

    elif type_ == "last_edited_time":
        return property["last_edited_time"]

    elif type_ == "last_edited_by":
        return property["last_edited_by"]["name"]

    raise NotImplementedError(f"unsupported property: {json.dumps(property)}")


def convert(property, values):
    """Convert a Notion property to a PostgreSQL column."""

    type_ = property["type"]

    if type_ == "title":
        return "text", values

    # Basic properties

    elif type_ == "rich_text":
        return "text", values

    elif type_ == "number":
        if all(isinstance(value, int) for value in values if value is not None):
            return "integer", values
        else:
            return "double precision", values

    elif type_ == "select":
        return "text", values

    elif type_ == "multi_select":
        return "text[]", values

    elif type_ == "date":
        if any(value[1] is not None for value in values):
            # This is a range of dates or datetimes.
            if all(
                DATE_RE.fullmatch(value[0]) for value in values if value[0] is not None
            ) and all(
                DATE_RE.fullmatch(value[1]) for value in values if value[1] is not None
            ):
                return "daterange", values
            else:
                return "tstzrange", values
        else:
            # This is a date or datetime.
            values = [value[0] for value in values]
            if all(DATE_RE.fullmatch(value) for value in values if value is not None):
                return "date", values
            else:
                return "timestamp with time zone", values

    elif type_ == "people":
        if all(len(value) <= 1 for value in values):
            return "uuid", [value[0] if value else None for value in values]
        else:
            return "uuid[]", values

    elif type_ == "files":
        if all(len(value) <= 1 for value in values):
            return "text", [value[0] if value else None for value in values]
        else:
            return "text[]", values

    elif type_ == "checkbox":
        return "boolean", values

    elif type_ == "url":
        return "text", values

    elif type_ == "email":
        return "text", values

    elif type_ == "phone_number":
        return "text", values

    # Advanced properties

    elif type_ == "formula":
        (subtype,) = set(value[0] for value in values)
        values = list(value[1] for value in values)
        if subtype == "date":
            if all(DATE_RE.fullmatch(value) for value in values if value is not None):
                return "date", values
            else:
                return "timestamp with time zone", values
        elif subtype == "number":
            if all(isinstance(value, int) for value in values if value is not None):
                return "integer", values
            else:
                return "double precision", values
        elif subtype == "string":
            return "text", values
        formula = property["formula"]
        raise NotImplementedError(f"unsupported formula: {json.dumps(formula)}")

    elif type_ == "relation":
        if all(len(value) <= 1 for value in values):
            return "uuid", [value[0] if value else None for value in values]
        else:
            return "uuid[]", values

    elif type_ == "rollup":
        (subtype,) = set(value[0] for value in values)
        values = list(value[1] for value in values)
        if subtype == "number":
            if all(isinstance(value, int) for value in values if value is not None):
                return "integer", values
            else:
                return "double precision", values
        rollup = property["rollup"]
        raise NotImplementedError(f"unsupported rollup: {json.dumps(rollup)}")

    elif type_ == "created_time":
        return "timestamp with time zone", values

    elif type_ == "created_by":
        return "text", values

    elif type_ == "last_edited_time":
        return "timestamp with time zone", values

    elif type_ == "last_edited_by":
        return "text", values

    raise NotImplementedError(f"unsupported property: {json.dumps(property)}")


def sanitize_name(name):
    """Convert a Notion property name to a PostgreSQL column name."""
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode()
    name = name.lower().strip().replace(" ", "_")
    name = INVALID_IN_NAME_RE.sub("", name)
    return name


def create_table(dsn, table_name, field_names, field_types, rows, drop, timestamp):
    """Create a PostgreSQL table."""
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            if timestamp is not None:
                view_name, table_name = table_name, table_name + timestamp

            if drop:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                logging.info("Dropped PostgreSQL table %s", table_name)

            columns = ", ".join(
                f"{name} {type}" for name, type in zip(field_names, field_types)
            )
            cursor.execute(f"CREATE TABLE {table_name} ({columns})")
            logging.info("Created PostgreSQL table %s", table_name)

            columns = ", ".join(field_names)
            with cursor.copy(f"COPY {table_name} ({columns}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(row)
            logging.info("Wrote %d rows to PostgreSQL", len(rows))

            if timestamp is not None:
                cursor.execute(
                    f"CREATE OR REPLACE VIEW {view_name} AS "
                    f"SELECT * from {table_name}"
                )
                logging.info("Created PostgreSQL view %s", view_name)

        connection.commit()


def sync_database(database_id, table_name, drop_existing=False, versioned=False):
    """Sync a database from Notion to PostgreSQL."""
    # Validate env vars.
    try:
        # Integration needs access to tables that will be synced and to every
        # table referenced by a relation or a rollup.
        token = os.environ["NOTION_TOKEN"]
    except KeyError:
        raise RuntimeError("missing environment variable NOTION_TOKEN") from None
    try:
        dsn = os.environ["POSTGRESQL_DSN"]
    except KeyError:
        raise RuntimeError("missing environment variable POSTGRESQL_DSN") from None

    # Validate arguments.
    DATABASE_ID_RE = re.compile(r"[0-9a-f]{32}")
    if not DATABASE_ID_RE.fullmatch(database_id):
        raise ValueError(
            f"invalid Notion database ID: {database_id}; "
            f"must match {DATABASE_ID_RE.pattern}"
        )
    # PostgreSQL supports 31 characters in a name. We need 14 for the timestamp.
    TABLE_NAME_RE = re.compile(r"[a-z_][a-z0-9_]+")
    if not TABLE_NAME_RE.fullmatch(table_name):
        raise ValueError(
            f"invalid PostgreSQL table name: {table_name}; "
            f"must match {TABLE_NAME_RE.pattern}"
        )
    TABLE_NAME_MAX_LENGTH = 17 if versioned else 31
    if len(table_name) > TABLE_NAME_MAX_LENGTH:
        raise ValueError(
            f"invalid PostgreSQL table name: {table_name}; "
            f"must contain no more than {TABLE_NAME_MAX_LENGTH} characters"
        )
    timestamp = datetime.datetime.utcnow().strftime("_%y%m%d_%H%M%S")

    # Read the Notion database structure and content in memory.
    database = get_database(database_id, token)
    pages = list(iter_database(database_id, token))

    # Convert to PostgreSQL field types and corresponding column values.
    field_names = ["id"]
    field_types = ["uuid"]
    columns = [[page["id"] for page in pages]]
    for name, property in database["properties"].items():
        assert name == property["name"]  # Notion duplicates this info
        values = [get_value(page["properties"][name]) for page in pages]
        field_type, column = convert(property, values)
        logging.info("Converted property %s to %s", name, field_type)
        field_names.append(sanitize_name(name))
        field_types.append(field_type)
        columns.append(column)

    rows = list(zip(*columns))

    # Write PostgreSQL table.
    create_table(
        dsn,
        table_name,
        field_names,
        field_types,
        rows,
        drop=drop_existing,
        timestamp=timestamp if versioned else None,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Import a Notion database to a PostgreSQL table"
    )
    parser.add_argument("database_id", help="Notion database ID")
    parser.add_argument("table_name", help="PostgreSQL table name")
    parser.add_argument(
        "--drop-existing", action="store_true", help="Drop table if it exists"
    )
    parser.add_argument(
        "--versioned", action="store_true", help="Import into a timestamped table"
    )
    sync_database(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
