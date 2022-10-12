import unittest
import pytest
import os
import pathlib
import sqlite3

from src.orm import Database, Table, Column, ForeignKey


class Test01_CreateDatabase(unittest.TestCase):

    def test_it(self):
        global db
        ROOT_DIR = pathlib.Path(__file__).absolute().parent
        db_name = 'test.db'
        db_path = f'{ROOT_DIR}{os.sep}{db_name}'

        if not os.path.exists(db_path):
            raise FileExistsError

        db = Database(db_path)

        assert isinstance(db.conn, sqlite3.Connection)

        assert db.tables == ['author', 'sqlite_sequence', 'post']


class Test02_DefineTables(Test01_CreateDatabase):

    def test_it(self):
        super().test_it()

        global Author, Post

        class Author(Table):
            name = Column(str)
            lucky_number = Column(int)

        class Post(Table):
            title = Column(str)
            published = Column(bool)
            author = ForeignKey(Author)

        assert Author.name.type == str
        assert Post.author.table == Author

        assert Author.name.sql_type == 'TEXT'
        assert Author.lucky_number.sql_type == 'INTEGER'


class Test03_CreateTables(Test02_DefineTables):
    def test_it(self):
        super().test_it()

        # db.create(Author)
        # db.create(Post)

        assert Author._get_create_sql() == "CREATE TABLE author (id INTEGER PRIMARY KEY AUTOINCREMENT, lucky_number INTEGER, name TEXT);"
        assert Post._get_create_sql() == "CREATE TABLE post (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER, published INTEGER, title TEXT);"


class Test04_CreateAuthorInstance(Test03_CreateTables):

    def test_it(self):
        super().test_it()
        global alex

        alex = Author(name='Alex', lucky_number=13)

        assert alex.name == 'Alex'
        assert alex.lucky_number == 13
        assert alex.id is None


class Test05_InsertAuthorInstance(Test03_CreateTables):

    def test_it(self):
        super().test_it()

        # assert alex.id is None

        db.save(alex)

        assert alex._get_insert_sql() == (
            'INSERT INTO author (lucky_number, name) VALUES (?, ?);',
            [13, 'Alex']
        )

        assert alex.id == 1


class Test06_GetAuthorInstance(Test05_InsertAuthorInstance):
    def test_it(self):
        super().test_it()

        alex_1 = db.get(Author, 1)
        assert type(alex_1) == Author
        assert Author._get_select_where_sql(id=1) == (
            "SELECT id, lucky_number, name FROM author WHERE id = ?;",
            ['id', 'lucky_number', 'name'],
            [1],
        )
        assert alex_1.name == 'Alex'
        assert alex_1.lucky_number == 13
