# simplesql3
This is a simple class I created to help me manage my sqlite3 databases in my projects. I've added some functionality to
to allow basic SQL statement generation for updating, inserting, and selecting. You can chain statements together to allow
somewhat complex statements.

## To get started just:
`from simplesql3 import simplesql3`
or
`from simplesql3 import simplesql3 as ss3`

## Some basic tests are:
```database_name, table_name, columns_dict
a = simplesql3("testing", "testing", {"a": "INTEGER", "b": "TEXT", "c": "TEXT", "d": "TEXT"})  # Creating a DB
b = simplesql3("testing", "testing2", {"e": "TEXT", "f": "TEXT", "g": "TEXT", "h": "TEXT"})  # Creating a new table
c = simplesql3("testing", "testing")  # Already existing DB
# Uncomment this to populate DB for testing
# for each in range(100):
#     a.insert(1+each, 2+each, 3+each, 4+each)
#     b.insert(1+each, 2+each, 3+each, 4+each)
print(f"--\nColumn Names:\n{a.column_names}, {b.column_names}, {c.column_names}")
print(f"--\nSelect All:\n", f"{a.statement}", a.select(select_all=True))

a.custom_sql("SELECT * FROM testing")
print(f"--\nCustom SQL:\n", f"{a.statement}", a.select())

a.getwhere("a", "c", query=4)
print(f"--\ngetwhere method:\n", f"{a.statement}", a.select())

a.getlike("a, b", "c", query=4)
print(f"--\ngetlike method:\n", f"{a.statement}", a.select())

a.getlike("a, b", "c").between((2, 30))
print(f"--\ngetlike between:\n", f"{a.statement}", a.select())

a.getlike("a, b", "c").between((2, 30), column="d")
print(f"--\ngetlike between (and):\n", f"{a.statement}", a.select())
```

For more tests, see the exampletest.py
