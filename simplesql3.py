import sqlite3
# Concern: Are the standard dictionaries an issue? Should I switch to ordered dict?
# How to automatically convert user input to ordered dict without losing order?
# Context manager with __enter__ & __exit__ methods
# https://stackoverflow.com/questions/1984325/explaining-pythons-enter-and-exit#1984346
# https://github.com/mikelane/ShowerThoughtBot/blob/master/dbmanager.py
# https://www.python.org/dev/peps/pep-0343/


class dbMGMT():
    """
    Adds simple context management to make resource management of the DB easier.
    """

    def __init__(self, db):
        self.db = db
        self.conn = None
        self.c = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.db)
        self.c = self.conn.cursor()

    def __exit__(self, exception_type, exception_value, traceback):
        self.c.close()
        self.conn.close()


class simplesql3():
    """Table/db created upon init

    If working with an existing DB, leave the columns_dict blank -
    column names will be populated automatically.

    database_name: looks for suffix '.db' - will add
    '.db' if not found. This can be overridden with the
    override boolean argument

    **columns_dict takes the column name and the type:
    'col_name':'TEXT'

    Wildcard search: query = "%" + query + "%"

    fetch_all=True, change to false to use fetchone() method

    No fetchmany() support at this time, probably a simple if statement

    TODO Add arg for returning in json
    """

    def __init__(self, database_name, table_name, columns_dict=None, override=False):
        if not database_name and table_name:
            raise AttributeError("One/more missing: database_name, table_name")

        if not columns_dict:
            columns_dict = dict()
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

        self.columns_dict = columns_dict

        if len(self.columns_dict) == 0:
            # Gets the column names for working with an existing DB
            conn = sqlite3.connect(self.database_name)
            c = conn.cursor()
            c.execute(f"""SELECT * FROM {self.table_name}""")
            self.column_names = list(map(lambda x: x[0], c.description))
            self.column_types = ["TEXT"] * len(self.column_names)
            c.close()
            conn.close()
        else:
            self.column_names = list(self.columns_dict.keys())
            self.column_types = list(columns_dict.values())

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
        if type in {"andis", "oris", "andlike", "orlike"} and isinstance(operator, list):
            operator_switch = {
                "andis": [f" AND {k} = ?" for k, v in operator],
                "oris": [f" OR {k} = ?" for k, v in operator],
                "andlike": [f" AND {k} LIKE ?" for k, v in operator],
                "orlike": [f" OR {k} LIKE ?" for k, v in operator]
            }
            operator = operator_switch[type]

        elif type is "between" and isinstance(operator, tuple):
            if arg is not None:
                operator = [f" AND {arg} BETWEEN ? AND ?"]
                return "".join(operator)
            operator = [f" BETWEEN ? AND ?"]
        else:
            raise TypeError("AND/OR operators argument must be list of tuples, BETWEEN must be a 2 value tuple")

        return "".join(operator)

    def getwhere(self, select_column, like_column, *, query=None):
        """Used for specific selections, from one or more
        columns:

        select_column = column1, column2
        like_column = column3
        SELECT column1, column2 FROM my_table WHERE column3 = query

        Can be chained to create more complex queries,
        if no query is supplied

        * in the args denotes that positional argument name is required,
        otherwise a TypeError will be raised
        """
        if query is not None:
            self._create_args(query)
            self.statement = f"SELECT {select_column} FROM {self.table_name} WHERE {like_column} = ?"
            return self
        self.statement = f"SELECT {select_column} FROM {self.table_name} WHERE {like_column}"
        return self

    def getlike(self, select_column, like_column, *, query=None):
        """Used for specific selections, from one or more
        columns:

        select_column = column1, column2
        like_column = column3
        SELECT column1, column2 FROM my_table WHERE column3 LIKE query

        Can be chained to create more complex queries,
        if no query is supplied

        * in the args denotes that positional argument name is required,
        otherwise a TypeError will be raised
        """
        if query is not None:
            self._create_args(query)
            self.statement = f"SELECT {select_column} FROM {self.table_name} WHERE {like_column} LIKE ?"
            return self
        self.statement = f"SELECT {select_column} FROM {self.table_name} WHERE {like_column}"
        return self

    def and_where(self, and_list, *, type=None):
        """Takes a dictionary
        {
        "column1": "thing1",
        "column2": "thing2"
        }
        this equates to 'AND column1 = thing1 AND column2 = thing2'
        The type argument will accept either 'like' or 'is' as arguements
        which will produce: 'AND column1 LIKE thing1' or 'AND column1 = thing1'

        'type' will default to '='

        * in the args denotes that positional argument name is required,
        otherwise a TypeError will be raised
        """
        self._create_args([v for k, v in and_list])
        if type is not None:
            if type is not "like":
                raise AttributeError("Type must be either: 'like' or 'is'")
            else:
                self.statement += self._check_operator(and_list, "andlike")
                return self
        else:
            self.statement += self._check_operator(and_list, "andis")
            return self

    def or_where(self, or_list, *, type=None):
        """Takes a dictionary
        {
        "column1": "thing1",
        "column2": "thing2"
        }
        this equates to 'OR column1 = thing1 OR column2 = thing2'
        The type argument will accept either 'like' or 'is' as arguements
        which will produce: 'OR column1 LIKE thing1' or 'OR column1 = thing1'

        'type' will default to '='

        * in the args denotes that positional argument name is required,
        otherwise a TypeError will be raised
        """
        self._create_args([v for k, v in or_list])
        if type is not None:
            if type is not "like":
                raise AttributeError("Type must be either: 'like' or 'is'")
            else:
                self.statement += self._check_operator(or_list, "orlike")
                return self
        self.statement += self._check_operator(or_list, "oris")
        return self

    def between(self, between_tuple, *, column=None):
        """Takes a tuple of exactly 2 values
        '(2018-09-30, 2018-10-01)'
        If a column is specified, then it will produce
        a statement like:
        'date_column BETWEEN 2018-09-30 AND 2018-10-01'

        Otherwise, it can be chained with other methods and
        will only produce 'BETWEEN 2018-09-30 AND 2018-10-01'

        * in the args denotes that positional argument name is required,
        otherwise a TypeError will be raised
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
            raise AttributeError("'where_dict' argument only receives one key/value pair. Chain with and_where/or_where methods for longer expressions")
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

    def __str__(self):
        return (f"""
        current statement: {self.statement}
        db name: {self.database_name}
        table name: {self.table_name}
        columns: {self.column_names}
        columns types: {self.column_types}
        args: {self.args}
        """)

    def __repr__(self):
        return (f"""
        current statement: {self.statement}
        db name: {self.database_name}
        table name: {self.table_name}
        columns: {self.column_names}
        columns types: {self.column_types}
        args: {self.args}
        """)


"""
https://mail.python.org/pipermail/python-dev/2003-October/038855.html
I find the chaining form a threat to readability; it requires that the
reader must be intimately familiar with each of the methods.

I'd like to reserve chaining for operations that return new values,
like string processing operations:

  y = x.rstrip("\n").split(":").lower()

"""
