from simplesql3 import simplesql3


# database_name, table_name, columns_dict
a = simplesql3("testing", "testing", {"a": "INTEGER", "b": "TEXT", "c": "TEXT", "d": "TEXT"})  # Creating a DB
b = simplesql3("testing", "testing2", {"e": "TEXT", "f": "TEXT", "g": "TEXT", "h": "TEXT"})  # Creating a new table
c = simplesql3("testing", "testing")  # Already existing DB
print(a, b, c)
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

a.getwhere("a, b, c", "c", query=4).and_where([("d", 5), ("a", 2)])
print(f"--\ngetwhere and_where:\n", f"{a.statement}", a.select())

a.getwhere("a, b", "c", query=4).or_where([("d", 5), ("a", 3)])
print(f"--\ngetwhere or_where:\n", f"{a.statement}", a.select())

a.getwhere("a, b", "c", query=4).and_where([("d", 5), ("a", 2)], type="like")
print(f"--\ngetwhere and_where(like):\n", f"{a.statement}", a.select())

a.getwhere("a, b", "c", query=4).or_where([("d", 5), ("a", 3)], type="like")
print(f"--\ngetwhere or_where(like):\n", f"{a.statement}", a.select())

a.getlike("a, b", "c", query=4).and_where([("d", 5), ("a", 3)]).or_where([("a", 5), ("b", 3)])
print(f"--\ngetlike and_where or_where:\n", f"{a.statement}", a.select())

a.getlike("a, b", "c", query=4).and_where([("d", 5), ("a", 3)], type="like").or_where([("a", 5), ("b", 3)], type="like")
print(f"--\ngetlike and_where(like) or_where(like):\n", f"{a.statement}", a.select())

a.update({"a": "a"}, {"a": 2})
print(f"--\nupdate\n", f"{a.statement}")
a.commit()

a.update({"a": "a", "b": "b", "c": "c", "d": "d"}, {"a": 1}).and_where([("b", 2), ("c", 3), ("d", 4)])
print(f"--\nupdate and_where:\n", f"{a.statement}")
a.commit()

b = simplesql3("testing", "testing3", {"status": "TEXT", "f": "TEXT", "g": "TEXT", "h": "TEXT"})  # Creating a new table

b.getwhere("a, b", "c", query=4).or_where([("status", "pending"), ("status", "open")], type="like")
print(f"--\ngetwhere or_where(like):\n", f"{a.statement}", a.select())
