notion2pg - Import Notion databases to PostgreSQL tables
========================================================

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

**Why does this project exist?**

There's a wide range of services offering synchronization from Notion to
PostgreSQL. However, they're based on clunky automation services requiring
configuration in a UI.

It was faster to write this script than to figure out how to use one of them.

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

Changelog
---------

0.1
~~~

* Initial public release.
