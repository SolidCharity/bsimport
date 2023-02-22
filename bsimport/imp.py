"""This module provides a layer between the CLI and the API wrapper."""
# bsimport/imp.py

from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple
from bsimport import EMPTY_FILE_ERROR, FILE_READ_ERROR, SUCCESS

from bsimport.wrapper import Bookstack

import sqlite3
import MySQLdb
import configparser


class IResponse(NamedTuple):
    """
    Represents a response from an Importer.
    Contains:
    - An error code.
    - The data from the request, e.g the book ID, an error message, etc.
    """
    error: int
    data: Any


class Importer():
    """
    A class to add a layer between the CLI and the wrapper.
    """

    def __init__(self, id: str, secret: str, url: str):
        self._wrapper = Bookstack(id, secret, url)

    def _parse_front_matter(
        self,
        content: List[str]
    ) -> Tuple[List[Dict[str, str]], int]:
        """
        Parse the YAML front matter in search of tags.

        :param content:
            The content of the Markdown file to parse.
        :type content: List[str]

        :return:
            The tags found, if any.
        :rtype: List[Dict[str, str]]
        :return:
            The position of the end of the front matter, if it exists.
        :rtype: int
        """

        tags = list()
        end = -1

        if not content[0].startswith('---'):
            return tags, end

        tmp = []

        for count, line in enumerate(content[1:]):
            # Found the end of the front matter
            if line.startswith('---'):
                end = count + 1
                break

            lc = line.split(':')

            # Improperly formatted or YAML list (not supported)
            if len(lc) == 1:
                continue
            # Skip all other front matter content
            if lc[0] != 'tags':
                continue

            # Retrieve the tags
            tmp = lc[1]
            tmp = tmp.rstrip().lstrip()
            tmp = tmp.rstrip(']')
            tmp = tmp.lstrip('[')
            tmp = tmp.split(', ')

        for tag in tmp:
            tags.append({'name': tag})

        return tags, end

    def _parse_file(
        self,
        content: List[str]
    ) -> Tuple[str, str, List[Dict[str, str]]]:
        """
        Parse the Markdown file to get the name from the title
        and the tags from the front matter.

        :param content:
            The content of the Markdown file to parse.
        :type content: List[str]

        :return:
            The name of the page, taken from the title of the file
            (H1 header).
        :rtype: str
        :return:
            The rest of the text, without the H1 header.
        :rtype: str
        :return:
            The tags found, if any.
        :rtype: List[Dict[str, str]]
        """
        tags, end = self._parse_front_matter(content)
        start = 0 if (end == -1) else (end + 1)

        text_start = -1
        name = ""
        for count, line in enumerate(content[start:]):
            if line.startswith('# '):
                name = line.rstrip()
                name = name.lstrip('# ')
            #if line.startswith('## ') and text_start == -1:
            elif text_start == -1:
                text_start = count + start
                break

        text = ''.join(content[text_start:])

        return name, text, tags

    def connect_sqlite(self):
        sq3 = sqlite3.connect('bsimport.sqlite3')
        sq3.execute("""
            CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            src_id INTEGER,
            bs_id INTEGER)""")
        sq3.execute("""
            CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            src_page_id INTEGER,
            bs_page_id INTEGER,
            bs_book_id INTEGER)""")
        sq3.commit()
        return sq3


    def connect_mysql(self):
        config = configparser.ConfigParser()
        path_my_cfg = str(Path(Path.home(), ".my.cnf"))
        config.read(path_my_cfg)

        mydb=MySQLdb.connect(database = config['mysql']['database'], read_default_file=path_my_cfg)
        return mydb


    def get_or_create_book(self, sq3, src_book_id):

        cursor = sq3.cursor()
        cursor.execute("SELECT bs_id FROM books WHERE src_id = ?", (src_book_id,))
        row = cursor.fetchone()
        if row is not None:
            return row[0]

        # create a new book
        # TODO: get the title of the book
        error, data = self.create_book(str(src_book_id))

        if error:
            print(f"Create book failed with: {error}")
            raise Exception(error)

        bs_id = data

        cursor.execute("INSERT INTO books(src_id, bs_id) VALUES(?,?)", (src_book_id,bs_id,))
        sq3.commit()

        return bs_id


    def get_or_create_page(self, sq3, bs_book_id, src_page_id, page_title):
        cursor = sq3.cursor()
        cursor.execute("SELECT bs_page_id FROM pages WHERE src_page_id = ? AND bs_book_id = ?", (src_page_id, bs_book_id))
        row = cursor.fetchone()
        if row is not None:
            return row[0]

        # create a new page
        tags = None
        error, data = self._wrapper.create_page(
            page_title, "EMPTY", tags, book_id=bs_book_id
        )

        if error:
            print(f"Create page failed with: {error}")
            raise Exception(error)

        bs_page_id = data

        cursor.execute("INSERT INTO pages(src_page_id, bs_page_id, bs_book_id) VALUES(?,?,?)", (src_page_id,bs_page_id,bs_book_id))
        sq3.commit()

        return bs_page_id


    def import_doc(
        self,
        file_path: Path
    ) -> IResponse:
        """
        import a document, and get information from the source mysql database about which book this page belongs to
        """

        mydb = self.connect_mysql()
        sq3 = self.connect_sqlite()

        src_page_id = int(file_path.name[0:file_path.name.index('-')])
        print(src_page_id, file_path)

        c=mydb.cursor()
        c.execute("""SELECT resource_book_id FROM resource_book_page
            WHERE resource_page_id = %s""", (src_page_id,))

        row = c.fetchone()
        first_book_page_id = None
        while row is not None:
            print(row)

            # does this book already exist?
            bs_book_id = self.get_or_create_book(sq3, row[0])

            page_title = file_path.stem

            bs_page_id = self.get_or_create_page(sq3, bs_book_id, src_page_id, page_title)

            if first_book_page_id is None:
                error, msg = self.import_page(file_path, book_id=bs_book_id, page_id=bs_page_id)
                if error:
                    return IResponse(error, msg)
                first_book_page_id = bs_page_id

            else:
                # create a page with a reference
                # see https://www.bookstackapp.com/docs/user/reusing-page-content/
                error, msg = self.import_page_text(page_title, "{{@" + str(first_book_page_id) + "}}", None, book_id=bs_book_id, page_id=bs_page_id)
                if error:
                    return IResponse(error, msg)

            row = c.fetchone()

        return IResponse(SUCCESS, "")

    def import_page_text(
        self,
        name: str,
        text: str,
        tags: Optional[List[Dict[str, str]]] = None,
        book_id: Optional[int] = -1,
        chapter_id: Optional[int] = -1,
        page_id: Optional[int] = -1,
    ) -> IResponse:
        """
        import the given text as a page.

        :param text:
            The text to import.
        :type text: str

        :param book_id:
            The ID of the book the page will be attached to.
            Required without `chapter_id`.
        :type book_id: Optional[int]
        :param chapter_id:
            The ID of the chapter the page will be attached to.
            Required without `book_id`.
        :type chapter_id: Optional[int]

        :return:
            An error code.
        :rtype: int
        :return:
            The name of the page if successful, the error message otherwise.
        :rtype: str
        """

        if page_id != -1:
            # update existing page
            error, msg = self._wrapper.update_page(
                book_id, page_id, name, text, tags
            )
        elif book_id != -1:
            error, msg = self._wrapper.create_page(
                name, text, tags, book_id=book_id
            )
        else:
            error, msg = self._wrapper.create_page(
                name, text, tags, chapter_id=chapter_id
            )

        if error:
            return IResponse(error, msg)
        else:
            return IResponse(SUCCESS, name)


    def import_page(
        self,
        file_path: Path,
        book_id: Optional[int] = -1,
        chapter_id: Optional[int] = -1,
        page_id: Optional[int] = -1,
    ) -> IResponse:
        """
        Parse a Markdown file and import it as a page.

        :param file_path:
            The path to the file to import.
        :type file_path: Path

        :param book_id:
            The ID of the book the page will be attached to.
            Required without `chapter_id`.
        :type book_id: Optional[int]
        :param chapter_id:
            The ID of the chapter the page will be attached to.
            Required without `book_id`.
        :type chapter_id: Optional[int]

        :return:
            An error code.
        :rtype: int
        :return:
            The name of the page if successful, the error message otherwise.
        :rtype: str
        """

        try:
            with file_path.open('r') as file:
                content = file.readlines()
        except OSError:
            return IResponse(FILE_READ_ERROR, "")

        if len(content) == 0:
            return IResponse(EMPTY_FILE_ERROR, "")

        name, text, tags = self._parse_file(content)

        if not name:
            name = file_path.stem

        if not tags:
            tags = None

        return self.import_page_text(name, text, tags, book_id, chapter_id, page_id)


    def import_chapter(
        self,
        path: Path,
        book_id: int
    ) -> IResponse:
        """
        Create a chapter from the directory's name.

        :param path:
            The path to the directory.
        :type path: Path
        :param book_id:
            The ID of the book the chapter belongs to.
        :type book_id: int

        :return:
            An error code.
        :rtype: int
        :return:
            The chapter's ID if successful, the error message otherwise.
        :rtype: Union[int, str]
        """

        name = path.stem

        # description = None
        # tags = None

        error, data = self._wrapper.create_chapter(book_id, name)

        if error:
            return IResponse(error, data)
        else:
            chapter_id = data
            return IResponse(SUCCESS, chapter_id)

    def import_book(
        self,
        path: Path
    ) -> IResponse:
        """
        Create a book from the directory's name.

        :param path:
            The path to the directory.
        :type path: Path

        :return:
            An error code.
        :rtype: int
        :return:
            The book's ID if successful, the error message otherwise.
        :rtype: Union[int, str]
        """

        name = path.stem

        # description = None
        # tags = None

        error, data = self._wrapper.create_book(name)

        if error:
            return IResponse(error, data)
        else:
            book_id = data
            return IResponse(SUCCESS, book_id)

    def create_book(
        self,
        name
    ) -> IResponse:
        """
        Create a book.

        :param name:
            The name of the book

        :return:
            An error code.
        :rtype: int
        :return:
            The book's ID if successful, the error message otherwise.
        :rtype: Union[int, str]
        """

        # description = None
        # tags = None

        error, data = self._wrapper.create_book(name)

        if error:
            return IResponse(error, data)
        else:
            book_id = data
            return IResponse(SUCCESS, book_id)

    def list_books(self) -> IResponse:
        """
        Get the list of all accessible books.

        :return:
            An error code.
        :rtype: int
        :return:
            If successful, a dictionnary of books with the book's ID
            as key and the book's name as value, the error message otherwise.
        :rtype: Union[Dict[int, str], str]
        """

        error, data = self._wrapper.list_books()

        if error:
            return IResponse(error, data)

        books = dict()
        for book in data:
            books[book['id']] = book['name']

        return IResponse(SUCCESS, books)
