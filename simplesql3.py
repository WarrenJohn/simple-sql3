import sqlite3


class simplesql3():
    """Table/db created upon init

    If working with an existing DB, leave the column_dict blank -
    column names will be populated automatically.

    database_name: looks for suffix '.db' - will add
    '.db' if not found. This can be overridden with the
    override boolean argument

    **column_dict takes the column name and the type:
    'col_name':'TEXT'

    Wildcard search: query = "%" + query + "%"

    fetch_all=True, change to false to use fetchone() method

    No fetchmany() support at this time, probably a simple if statement

    TODO Add arg for returning in json
    """

    def __init__(self, database_name, table_name, column_dict=dict(), override=False):
        assert database_name and table_name is not None, "One/more missing: database_name, table_name"

        self.database_name = database_name
        self.table_name = table_name
        self.override = override
        self.statement = str()
        self.args = list()

        if not override:
            if not self.database_name.endswith(".db"):
                self.database_name = f"{database_name}.db"
        else:
            self.database_name = database_name

        self.column_dict = column_dict

        if len(self.column_dict) is 0:
            # Gets the column names for working with an existing DB
            conn = sqlite3.connect(self.database_name)
            c = conn.cursor()
            c.execute(f"""SELECT * FROM {self.table_name}""")
            self.column_names = list(map(lambda x: x[0], c.description))
            self.column_types = ["TEXT"] * len(self.column_names)
            c.close()
            conn.close()
        else:
            self.column_names = list(self.column_dict.keys())
            self.column_types = list(column_dict.values())

        col_vals = "?," * len(str(self.column_names).split(","))
        self.col_vals = col_vals[:-1]

        self.columns = "".join([f"{a} {b.upper()}, " for a, b in zip(self.column_names, self.column_types)])[:-2]
        # Last 2 chars are always a space and comma

        conn = sqlite3.connect(self.database_name)
        c = conn.cursor()
        c.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} ({self.columns})")
        c.close()
        conn.close()

    def _create_args(self, arg):
        if isinstance(arg, list):
            self.args.extend(arg)
        else:
            self.args.append(arg)
        return self

    def _check_operator(self, operator, type, arg=None):
        if type in {"andis", "oris", "andlike", "orlike"} and isinstance(operator, dict):
            operator_switch = {
                "andis": [f" AND {k} = ?" for k, v in operator.items()],
                "oris": [f" OR {k} = ?" for k, v in operator.items()],
                "andlike": [f" AND {k} LIKE ?" for k, v in operator.items()],
                "orlike": [f" OR {k} LIKE ?" for k, v in operator.items()]
            }
            operator = operator_switch[type]

        elif type is "between" and isinstance(operator, tuple):
            if arg is not None:
                operator = [f" AND {arg} BETWEEN ? AND ?"]
                return "".join(operator)
            operator = [f" BETWEEN ? AND ?"]
        else:
            raise TypeError("AND/OR operators argument must be dictionary, BETWEEN must be a 2 value tuple")

        return "".join(operator)

    def getwhere(self, select_column, like_column, query=None):
        """Used for specific selections, from one or more
        columns:

        select_column = column1, column2
        like_column = column3
        SELECT column1, column2 FROM my_table WHERE column3 = query

        Can be chained to create more complex queries,
        if no query is supplied
        """
        if query is not None:
            self._create_args(query)
            self.statement = f"SELECT {select_column} FROM {self.table_name} WHERE {like_column} = ?"
            return self
        self.statement = f"SELECT {select_column} FROM {self.table_name} WHERE {like_column}"
        return self

    def getlike(self, select_column, like_column, query=None):
        """Used for specific selections, from one or more
        columns:

        select_column = column1, column2
        like_column = column3
        SELECT column1, column2 FROM my_table WHERE column3 LIKE query

        Can be chained to create more complex queries,
        if no query is supplied
        """
        if query is not None:
            self._create_args(query)
            self.statement = f"SELECT {select_column} FROM {self.table_name} WHERE {like_column} LIKE ?"
            return self
        self.statement = f"SELECT {select_column} FROM {self.table_name} WHERE {like_column}"
        return self

    def and_do(self, and_dict, type=None):
        """Takes a dictionary
        {
        "column1": "thing1",
        "column2": "thing2"
        }
        this equates to 'AND column1 = thing1 AND column2 = thing2'
        The type argument will accept either 'like' or 'is' as arguements
        which will produce: 'AND column1 LIKE thing1' or 'AND column1 = thing1'

        'type' will default to '='
        """
        self._create_args([v for k, v in and_dict.items()])
        if type is not None:
            if type is not "like":
                raise AttributeError("Type must be either: 'like' or 'is'")
            else:
                self.statement += self._check_operator(and_dict, "andlike")
                return self
        else:
            self.statement += self._check_operator(and_dict, "andis")
            return self

    def or_do(self, or_dict, type=None):
        """Takes a dictionary
        {
        "column1": "thing1",
        "column2": "thing2"
        }
        this equates to 'OR column1 = thing1 OR column2 = thing2'
        The type argument will accept either 'like' or 'is' as arguements
        which will produce: 'OR column1 LIKE thing1' or 'OR column1 = thing1'

        'type' will default to '='
        """
        self._create_args([v for k, v in or_dict.items()])
        if type is not None:
            if type is not "like":
                raise AttributeError("Type must be either: 'like' or 'is'")
            else:
                self.statement += self._check_operator(or_dict, "orlike")
                return self
        self.statement += self._check_operator(or_dict, "oris")
        return self

    def between(self, between_tuple, column=None):
        """Takes a tuple of exactly 2 values
        '(2018-09-30, 2018-10-01)'
        If a column is specified, then it will produce
        a statement like:
        'date_column BETWEEN 2018-09-30 AND 2018-10-01'

        Otherwise, it can be chained with other methods and
        will only produce 'BETWEEN 2018-09-30 AND 2018-10-01'
        """
        if len(between_tuple) != 2:
            raise ValueError("BETWEEN may only accept 2 values")
        self._create_args([v for v in between_tuple])
        if column is not None:
            self.statement += self._check_operator(between_tuple, "between", column)
            return self
        self.statement += self._check_operator(between_tuple, "between")
        return self

    def insert(self, *args):
        args = tuple(args)
        assert len(args) == len(self.col_vals.split(',')), f"{len(args)} argument and {len(self.col_vals.split(','))} column - length do not match"
        conn = sqlite3.connect(self.database_name)
        c = conn.cursor()
        c.execute(f"""INSERT INTO {self.table_name} VALUES ({self.col_vals})""", args)
        conn.commit()
        c.close()
        conn.close()
        # return self

    def insertrow(self, *args):
        """For chunking data into the DB
        useful for use with a csv
        https://stackoverflow.com/questions/5942402/python-csv-to-sqlite/7137270#7137270
        """
        # TODO
        pass

    def select(self, select_all=False, fetch_all=True):
        data = tuple()
        conn = sqlite3.connect(self.database_name)
        c = conn.cursor()
        if select_all:
            self.statement = f"SELECT * FROM {self.table_name}"
            # Done separately so the user can still call the self.statement variable if needed
            c.execute(self.statement)
            if fetch_all:
                data = c.fetchall()
            else:
                data = c.fetchone()
        else:
            c.execute(self.statement, tuple(self.args))

            if fetch_all:
                data = c.fetchall()
            else:
                data = c.fetchone()
        c.close()
        conn.close()
        self.args = list()
        self.statement = str()
        return data

    def commit(self):
        """Use with the update or custom_sql methods
        """
        conn = sqlite3.connect(self.database_name)
        c = conn.cursor()
        c.execute(self.statement, tuple(self.args))
        conn.commit()
        c.close()
        conn.close()
        self.args = list()
        self.statement = str()

    def custom_sql(self, sql_statement):
        """Chained with either the .select() method,
        or the .commit() method.
        """
        self.statement = sql_statement
        return self

    def update(self, set_dict, where_dict):
        """Takes 2 dictionaries, set_dict will take many arguments
        but where_dict only accepts one key/value pair
        """
        if len(where_dict) > 1:
            raise AttributeError("'where_dict' argument only receives one key/value pair. Chain with and_do/or_do methods for longer expressions")
        self._create_args([v for k, v in set_dict.items()])
        self._create_args([v for k, v in where_dict.items()])
        self.statement = f"UPDATE {self.table_name} SET"
        if len(set_dict) > 1:
            self.statement += "".join([f" {k} = ?," for k, v in set_dict.items()])
            self.statement = self.statement[:-1]
        else:
            self.statement += "".join([f" {k} = ?" for k, v in set_dict.items()])
        self.statement += "".join([f" WHERE {k} = ?" for k, v in where_dict.items()])
        print(self.statement)
        return self


if __name__ == '__main__':
    # database_name, table_name, column_dict
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

    a.getwhere("a", "c", 4)
    print(f"--\ngetwhere method:\n", f"{a.statement}", a.select())

    a.getlike("a, b", "c", 4)
    print(f"--\ngetlike method:\n", f"{a.statement}", a.select())

    a.getlike("a, b", "c").between((2, 30))
    print(f"--\ngetlike between:\n", f"{a.statement}", a.select())

    a.getlike("a, b", "c").between((2, 30), "d")
    print(f"--\ngetlike between (and):\n", f"{a.statement}", a.select())

    a.getwhere("a, b, c", "c", 4).and_do({"d": 5, "a": 2})
    print(f"--\ngetwhere and_do:\n", f"{a.statement}", a.select())

    a.getwhere("a, b", "c", 4).or_do({"d": 5, "a": 3})
    print(f"--\ngetwhere or_do:\n", f"{a.statement}", a.select())

    a.getwhere("a, b", "c", 4).and_do({"d": 5, "a": 2}, "like")
    print(f"--\ngetwhere and_do(like):\n", f"{a.statement}", a.select())

    a.getwhere("a, b", "c", 4).or_do({"d": 5, "a": 3}, "like")
    print(f"--\ngetwhere or_do(like):\n", f"{a.statement}", a.select())

    a.getlike("a, b", "c", 4).and_do({"d": 5, "a": 3}).or_do({"a": 5, "b": 3})
    print(f"--\ngetlike and_do or_do:\n", f"{a.statement}", a.select())

    a.getlike("a, b", "c", 4).and_do({"d": 5, "a": 3}, "like").or_do({"a": 5, "b": 3}, "like")
    print(f"--\ngetlike and_do(like) or_do(like):\n", f"{a.statement}", a.select())

    a.update({"a": "a"}, {"a": 2})
    print(f"--\nupdate\n", f"{a.statement}")
    a.commit()

    a.update({"a": "a", "b": "b", "c": "c", "d": "d"}, {"a": 1}).and_do({"b": 2, "c": 3, "d": 4})
    print(f"--\nupdate and_do:\n", f"{a.statement}")
    a.commit()

    """
    https://mail.python.org/pipermail/python-dev/2003-October/038855.html
    I find the chaining form a threat to readability; it requires that the
    reader must be intimately familiar with each of the methods.

    I'd like to reserve chaining for operations that return new values,
    like string processing operations:

      y = x.rstrip("\n").split(":").lower()

    """
