import inspect
import sqlite3
import typing

SQLITE_TYPE_MAP = {
    int: 'INTEGER',
    float: 'REAL',
    str: 'TEXT',
    bytes: 'BLOB',
    bool: 'INTEGER',
}
CREATE_TABLE_SQL = "CREATE TABLE {name} ({fields});"
SELECT_TABLES_SQL = "SELECT name FROM sqlite_master WHERE type = 'table';"
INSERT_SQL = 'INSERT INTO {name} ({fields}) VALUES ({placeholders});'
SELECT_ALL_SQL = 'SELECT {fields} FROM {name};'
SELECT_WHERE_SQL = 'SELECT {fields} FROM {name} WHERE {query};'


class Table:
    def __init__(self, **kwargs) -> None:
        self._data = {
            'id': None
        }
        for key, value in kwargs.items():
            self._data[key] = value

    def __getattribute__(self, key):
        _data = object.__getattribute__(self, '_data')
        if key in _data:
            return _data[key]
        return object.__getattribute__(self, key)

    @classmethod
    def _get_name(cls):
        return cls.__name__.lower()

    @classmethod
    def _get_fields(cls):
        fields = [
            ('id', "INTEGER PRIMARY KEY AUTOINCREMENT")
        ]

        for name, field in inspect.getmembers(cls):
            if isinstance(field, Column):
                fields.append((name, field.sql_type))
            elif isinstance(field, ForeignKey):
                fields.append((f'{name}_id', "INTEGER"))
        return [' '.join(x) for x in fields]

    @classmethod
    def _get_create_sql(cls):
        return CREATE_TABLE_SQL.format(
            name=cls._get_name(),
            fields=', '.join(cls._get_fields())
        )

    @classmethod
    def _get_select_all_sql(cls):
        fields = ['id']
        for name, field in inspect.getmembers(cls):
            if isinstance(field, Column):
                fields.append(name)

        sql = SELECT_ALL_SQL.format(
            name=cls._get_name(),
            fields=', '.join(fields)
        )
        return sql, fields

    @classmethod
    def _get_select_where_sql(cls, **kwargs):
        fields = ['id']
        for name, field in inspect.getmembers(cls):
            if isinstance(field, Column):
                fields.append(name)

        filters = []
        params = []
        for key, value in kwargs.items():
            filters.append(key + ' = ?')
            params.append(value)

        sql = SELECT_WHERE_SQL.format(
            name=cls._get_name(),
            fields=', '.join(fields),
            query=' AND '.join(filters)
        )
        return sql, fields, params

    def _get_insert_sql(self) -> typing.Tuple[str, typing.Iterable]:
        fields = []
        placeholders = []
        values = []
        for name, field in inspect.getmembers(self.__class__):
            if isinstance(field, Column):
                fields.append(name)
                values.append(getattr(self, name))
                placeholders.append('?')
        sql = INSERT_SQL.format(
            name=self.__class__._get_name(),
            fields=', '.join(fields),
            placeholders=', '.join(placeholders)
        )
        return sql, values


class Column:
    def __init__(self, column_type):
        self.type = column_type

    @property
    def sql_type(self):
        return SQLITE_TYPE_MAP[self.type]


class ForeignKey:
    def __init__(self, table: type[Table]) -> None:
        self.table = table


class Database:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.Connection(path)

    def _execute(
            self,
            sql: str,
            parameters: typing.Optional[typing.Iterable] = None
    ) -> sqlite3.Cursor:
        if parameters:
            return self.conn.execute(sql, parameters)
        return self.conn.execute(sql)

    def create(self, table: type[Table]):
        self._execute(table._get_create_sql())

    def save(self, instance: type[Table]):
        sql, value = instance._get_insert_sql()
        cursor = self._execute(sql, value)
        instance._data['id'] = cursor.lastrowid

    def all(self, table: type[Table]):
        sql, fields = table._get_select_all_sql()
        result = []
        for row in self._execute(sql).fetchall():
            data = dict(zip(fields, row))
            result.append(table(**data))
        return result

    def get(self, table: type[Table], id: int):
        sql, fields, parameters = table._get_select_where_sql(id=id)
        row = self._execute(sql, parameters).fetchone()
        data = dict(zip(fields, row))
        return table(**data)

    @property
    def tables(self) -> typing.List:
        return [row[0] for row in self._execute(SELECT_TABLES_SQL).fetchall()]


