notion2pg - Import Notion databases to PostgreSQL tables
========================================================

When a system built with Notion_ databases reaches a sufficient scale, the need
for business intelligence arises. This requires extracting data from Notion and
loading it into a relational database.

The original author didn't find a convenient, off-the-shelf solution for this.
Services offering synchronization from Notion to a relational database rely on
clunky automations and involve manual configuration.

Thus notion2pg was born.

It does exactly one thing: convert any Notion database to a PostgreSQL table.
It requires zero configuration. You made changes in Notion? No worries, just
re-run notion2pg to refresh the table definition and its content.

.. _Notion: https://www.notion.so/
.. _PostgreSQL: https://www.postgresql.org/

While notion2pg is currently alpha software, it imported successfully complex
databases with dozens of columns and thousands of rows. There's a fair chance
that it will handle any human-sized Notion database.

Quick start
-----------

1. `Create a Notion integration`_.

   .. _Create a Notion integration: https://www.notion.so/my-integrations

2. Share a Notion database with your integration, as well as related databases.

3. Create a PostgreSQL database e.g.:

   .. code-block:: shell-session

      $ createuser notion
      $ createdb notion -O notion

4. Install notion2pg (requires Python ≥ 3.8):

   .. code-block:: shell-session

      $ pip install notion2pg

5. Set Notion and PostgreSQL credentials as environment variables e.g.:

   .. code-block:: shell-session

      $ export NOTION_TOKEN=secret_...
      $ export POSTGRESQL_DSN="dbname=notion user=notion"

6. Import your database e.g.:

   .. code-block:: shell-session

      $ notion2pg <database_id> <table_name>

   where ``<database_id>`` can be found in the URL of your database — it's a
   UUID like ``858611286a7d43a197c7c0ddcc7d5a4f`` and ``<table_name>`` is any
   valid PostgreSQL table name.

Command line options
--------------------

``--drop-existing``
~~~~~~~~~~~~~~~~~~~

Drop the PostgreSQL table if it exists. This is useful if you want to import a
table repeatedly, overwriting any previous version.

``--versioned``
~~~~~~~~~~~~~~~

Append a timestamp to the name of the PostgreSQL table. Then, create a view
pointing to that table, so it can still be queried under ``<table name>``. This
is useful if you want to import a table a repeatedly, but would rather keep
previous versions around.

FAQ
---

**Why is my relation or rollup field empty?**

Your integration must have access not only to the table that you're importing,
but also to every table involved in a relation or a rollup.

Limitations
-----------

* The order of columns in the table isn't preserved. This information isn't
  available in the API of Notion.
* Rollups "Show original" and "Show unique values" are ignored. Import the
  related table and join it in your queries instead.
* Properties of type "people" are imported as the person ID, which is probably
  not the most useful representation.
* Every import is a full copy. Given that Notion's API isn't particularly fast,
  the practical limit is around 10,000 rows.

Changelog
---------

0.1
~~~

* Initial public release.
