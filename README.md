TrailDB for SQLite
------------------

This repository contains an implementation of an extension that exposes
TrailDBs as virtual tables in SQLite.

Compile
-------

There is a `Makefile` in this repository that works on UNIXy systems. Install
SQLite and [TrailDB](https://traildb.io/) first.

```
    $ make
```

If compilation is successful, you will have a file `sqlite3traildb.so` in the
current directory that implements the extension.

The `Makefile` for this project is very simple, since this is a single C-file project. If something fails, the command to compile this extension is not much more than:

```
    $ gcc -o sqlite3traildb.so sqlite3_traildb.c -O2 -shared -fPIC -ltraildb -lsqlite3
```

Load
----

In SQLite, use `.load` or `LOAD_EXTENSION()` function to load up the extension.

```sql
    sqlite> .load ./sqlite3traildb
```

To load a TrailDB as a virtual table, invoke `CREATE VIRTUAL TABLE`:

```sql
    sqlite> CREATE VIRTUAL TABLE mytraildb USING traildb ('./path/to/traildb');
    sqlite> SELECT * FROM mytraildb;
```

Performance
-----------

This extension does not implement any indexes or special optimizations when
querying TrailDBs. This means most queries will scan the entire TrailDB.

TrailDBs are encoded as integers; this extension will only decode them where
necessary. For example, `SELECT COUNT(1) FROM tdb` will not decode any values
to strings.

Unimplemented features
----------------------

This extension cannot create TrailDBs. It can only read existing TrailDBs.

License
-------

This extension is MIT licensed.

How to contribute
-----------------

Open issues and pull requests on the [project GitHub page](https://github.com/traildb/traildb-sqlite3).

Contributing to this repository will require a signature on a Community License
Agreement; we have set up a CLA assistant to make that easy. It will pop up
when you open a pull request.
