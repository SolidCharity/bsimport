"""This module provides a layer between the CLI and the API wrapper."""
# bsimport/imp.py

from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple
from bsimport import EMPTY_FILE_ERROR, FILE_READ_ERROR, SUCCESS

from bsimport.wrapper import Bookstack

import sqlite3
import MySQLdb
import base64
import hashlib
import configparser
import re

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
            The title
        :rtype: str
        :return:
            The header
        :rtype: str
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
        header = ""
        title = ""

        for count, line in enumerate(content[1:]):
            # Found the end of the front matter
            if line.startswith('---'):
                end = count + 1
                break

            header += "    " + line

            lc = line.split(':')

            # Improperly formatted or YAML list (not supported)
            if len(lc) == 1:
                continue

            if lc[0] == 'Title':
                title = lc[1].strip()

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

        return title, header, tags, end

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
        name, header, tags, end = self._parse_front_matter(content)
        start = 0 if (end == -1) else (end + 1)

        text_start = -1
        for count, line in enumerate(content[start:]):
            if not name and line.startswith('# '):
                name = line.rstrip()
                name = name.lstrip('# ')
            #if line.startswith('## ') and text_start == -1:
            elif text_start == -1:
                text_start = count + start
                break

        text = ''.join(content[text_start:])

        if header:
            text = '---\n' + header + '---\n\n' + text

        return name, text, tags

    def connect_sqlite(self):
        sq3 = sqlite3.connect('bsimport.sqlite3')
        sq3.execute("""
            CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            src_id INTEGER,
            bs_id INTEGER,
            bs_slug VARCHAR)""")
        sq3.execute("""
            CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            src_page_id INTEGER,
            bs_page_id INTEGER,
            bs_book_id INTEGER,
            bs_book_slug VARCHAR,
            bs_page_slug VARCHAR,
            bs_page_title VARCHAR,
            content_md5sum VARCHAR)""")
        sq3.execute("""
            CREATE TABLE IF NOT EXISTS attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename VARCHAR,
            bs_page_id INTEGER,
            bs_att_id INTEGER)""")
        sq3.commit()
        return sq3


    def connect_mysql(self):
        config = configparser.ConfigParser()
        path_my_cfg = str(Path(Path.home(), ".my.cnf"))
        config.read(path_my_cfg)

        mydb=MySQLdb.connect(database = config['mysql']['database'], read_default_file=path_my_cfg)
        return mydb


    def first_page_of_book(self, mydb, src_book_id, src_page_id):
        c=mydb.cursor()
        c.execute("""SELECT resource_page_id FROM resource_book_page
            WHERE resource_book_id = %s
            ORDER BY display_order""", (src_book_id,))

        row = c.fetchone()
        if row is not None:
            return row[0] == src_page_id

        return False

    def get_or_create_book(self, sq3, src_book_id, page_title):

        cursor = sq3.cursor()
        cursor.execute("SELECT bs_id, bs_slug FROM books WHERE src_id = ?", (src_book_id,))
        row = cursor.fetchone()
        if row is not None:
            return (row[0], row[1])

        # create a new book
        error, data = self.create_book(page_title)

        if error:
            print(f"Create book failed with: {error}")
            raise Exception(error)

        bs_id = data
        bs_slug = page_title.lower().replace(' ', '-')

        cursor.execute("INSERT INTO books(src_id, bs_id, bs_slug) VALUES(?,?,?)", (src_book_id,bs_id,bs_slug,))
        sq3.commit()

        return (bs_id, bs_slug)


    def get_page(self, sq3, bs_book_id, src_page_id):
        cursor = sq3.cursor()
        cursor.execute("SELECT bs_page_id FROM pages WHERE src_page_id = ? AND bs_book_id = ?", (src_page_id, bs_book_id))
        row = cursor.fetchone()
        if row is not None:
            return row[0]

        return -1


    def remember_page(self, sq3, bs_book_id, bs_book_slug, src_page_id, bs_page_id, bs_page_slug, bs_page_title, content):
        cursor = sq3.cursor()
        content_md5sum = hashlib.md5(content.encode('utf-8')).hexdigest()
        # print(f"{bs_page_id}", content_md5sum)
        cursor.execute("INSERT INTO pages(src_page_id, bs_page_id, bs_book_id, bs_book_slug, bs_page_slug, bs_page_title, content_md5sum) VALUES(?,?,?,?,?,?,?)",
            (src_page_id,bs_page_id,bs_book_id,bs_book_slug,bs_page_slug,bs_page_title,content_md5sum))
        sq3.commit()


    def check_for_new_content(self, sq3, bs_book_id, bs_page_id, content):
        cursor = sq3.cursor()
        content_md5sum = hashlib.md5(content.encode('utf-8')).hexdigest()
        cursor.execute("SELECT content_md5sum FROM pages WHERE bs_book_id = ? AND bs_page_id = ?",
            (bs_book_id,bs_page_id,))
        row = cursor.fetchone()
        if row is not None:
            if row[0] == content_md5sum:
                return False
            # print(f"{bs_page_id}", content_md5sum)
            cursor.execute("UPDATE pages SET content_md5sum = ? WHERE bs_book_id = ? AND bs_page_id = ?",
                (content_md5sum, bs_book_id, bs_page_id,))
            sq3.commit()

        return True


    def import_attachments(
        self,
        sq3,
        path: Path,
        bs_page_id: int,
    ) -> IResponse:
        """
        import attachments and images for the given page from the given directory
        """

        if not path.exists():
            return IResponse(SUCCESS, "")

        for child in path.iterdir():
            if child.is_file():
                cursor = sq3.cursor()
                cursor.execute("SELECT id FROM attachments WHERE filename = ? and bs_page_id = ?", (child.name,bs_page_id,))
                row = cursor.fetchone()
                if row is not None:
                    continue

                error, data = self._wrapper.create_attachment(
                    child.name, child, bs_page_id)

                if error:
                    return IResponse(error, data)

                bs_att_id = data
                cursor.execute("INSERT INTO attachments(filename, bs_page_id, bs_att_id) VALUES(?,?,?)", (child.name,bs_page_id,bs_att_id))
                sq3.commit()

        return IResponse(SUCCESS, "")


    def import_books(
        self,
        path: Path
    ) -> IResponse:
        """
        use the table "resource_book_page" from the mysql database to walk through all books in the right order
        and import the documents
        """

        mydb = self.connect_mysql()
        sq3 = self.connect_sqlite()

        # read all existing documents
        pages = {}
        for child in Path(path, "docs").iterdir():
            if child.is_file() and child.suffix == '.md':
                src_page_id = int(child.name[0:child.name.index('-')])
                pages[src_page_id] = child

        # get all documents per book and in the right order
        c=mydb.cursor()
        c.execute("""SELECT resource_page_id FROM resource_book_page
            ORDER BY resource_book_id, display_order""")

        row = c.fetchone()
        while row is not None:
            if row[0] in pages:
                error, msg = self.import_doc(mydb, sq3, path, pages[row[0]])

                if error:
                    return IResponse(error, msg)

            row = c.fetchone()

        return IResponse(SUCCESS, "")


    def import_doc(
        self,
        mydb,
        sq3,
        import_path: Path,
        file_path: Path
    ) -> IResponse:
        """
        import a document, and get information from the source mysql database about which book this page belongs to
        """

        src_page_id = int(file_path.name[0:file_path.name.index('-')])
        print(src_page_id, file_path)

        c=mydb.cursor()
        c.execute("""SELECT resource_book_id FROM resource_book_page
            WHERE resource_page_id = %s""", (src_page_id,))

        row = c.fetchone()
        first_book_page_id = None
        while row is not None:

            # does this book already exist?
            src_book_id = row[0]
            book_title = str(src_book_id)
            if self.first_page_of_book(mydb, src_book_id, src_page_id):
                book_title = self.parse_page_title(file_path)
            (bs_book_id,bs_book_slug) = self.get_or_create_book(sq3, src_book_id, book_title)

            bs_page_id = self.get_page(sq3, bs_book_id, src_page_id)

            if first_book_page_id is None:

                error, msg = self.import_page(mydb, sq3, file_path, Path(import_path, "images", str(src_page_id)),
                    book_id=bs_book_id,
                    page_id=bs_page_id,
                    src_page_id=src_page_id,
                    book_slug=bs_book_slug)
                if error:
                    return IResponse(error, msg)
                bs_page_id = msg
                first_book_page_id = bs_page_id

                error, msg = self.import_attachments(sq3, Path(import_path, "files", str(src_page_id)), bs_page_id)
                if error:
                    return IResponse(error, msg)

            else:
                # get details of original page
                orig_page_slug = file_path.stem
                orig_page_title = file_path.stem
                cursor = sq3.cursor()
                cursor.execute("SELECT bs_page_slug, bs_page_title FROM pages WHERE bs_page_id = ?", (first_book_page_id,))
                row = cursor.fetchone()
                if row is not None:
                    orig_page_slug = row[0]
                    orig_page_title = row[1]


                # create a page with a reference
                # see https://www.bookstackapp.com/docs/user/reusing-page-content/
                error, msg = self.import_page_text(sq3, orig_page_title, "{{@" + str(first_book_page_id) + "}}", None,
                    book_id=bs_book_id,
                    page_id=bs_page_id,
                    src_page_id = src_page_id,
                    book_slug = bs_book_slug,
                    page_slug = orig_page_slug)
                if error:
                    return IResponse(error, msg)

            row = c.fetchone()

        return IResponse(SUCCESS, "")

    def import_page_text(
        self,
        sq3,
        name: str,
        text: str,
        tags: Optional[List[Dict[str, str]]] = None,
        book_id: Optional[int] = -1,
        chapter_id: Optional[int] = -1,
        src_page_id: Optional[int] = -1,
        page_id: Optional[int] = -1,
        book_slug: Optional[str] = None,
        page_slug: Optional[str] = None,
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

        error = False
        if page_id != -1:
            if self.check_for_new_content(sq3, book_id, page_id, text):
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
            if page_id == -1:
                # page was created
                page_id = int(msg)
                self.remember_page(sq3, book_id, book_slug, src_page_id, page_id, page_slug, name, text)

            return IResponse(SUCCESS, page_id)


    def parse_page_title(
        self,
        file_path: Path
    ):
        try:
            with file_path.open('r') as file:
                content = file.readlines()
        except OSError:
            return IResponse(FILE_READ_ERROR, "")

        if len(content) == 0:
            return IResponse(EMPTY_FILE_ERROR, "")

        title, text, tags = self._parse_file(content)

        return title

    def import_page(
        self,
        mydb,
        sq3,
        file_path: Path,
        images_path: Path,
        book_id: Optional[int] = -1,
        chapter_id: Optional[int] = -1,
        src_page_id: Optional[int] = -1,
        page_id: Optional[int] = -1,
        book_slug: Optional[str] = None,
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

        # embed images
        # Any images included via base64 data URIs will be extracted and saved as gallery images against the page during upload.
        if images_path.exists():
            for child in images_path.iterdir():
                if child.is_file() and child.name.startswith(str(src_page_id) + '-'):
                    fh = open(child, 'rb')
                    content = bytearray(fh.read())
                    encoded = base64.b64encode(content).decode('ascii')
                    extension = child.suffix.replace('.', '')
                    image = f'(data:image/{extension};base64,{encoded})'
                    text = text.replace(f"(../images/{src_page_id}/{child.name})",f"{image}")

        # update internal links, even between books
        # (../docs/123-my-page-title)
        pos = 0
        while '(../docs/' in text[pos:]:
            pos += text[pos:].index('(../docs/') + len('(../docs/')
            target = text[pos:]
            target = target[0:target.index(')')]
            target_page_id = int(target[0:target.index('-')])

            # this page belongs to which book?
            cursor = sq3.cursor()
            cursor.execute("SELECT bs_book_slug, bs_page_slug FROM pages WHERE src_page_id = ?", (target_page_id,))
            row = cursor.fetchone()
            if row is not None:
                bs_book_slug = row[0]
                bs_page_slug = row[1]
                text = text.replace(f'(../docs/{target})', f'(/books/{bs_book_slug}/page/{bs_page_slug})')

        # update links to attachments
        # (../files/465/456-my-document.pdf)
        pos = 0
        while '(../files/' in text[pos:]:
            pos += text[pos:].index('(../files/') + len('(../files/')
            target = text[pos:]
            target = target[0:target.index(')')]
            filename = target[target.index('/') + 1:]

            # get the id of the attachment
            cursor = sq3.cursor()
            cursor.execute("SELECT bs_att_id FROM attachments WHERE filename = ?", (filename,))
            row = cursor.fetchone()
            if row is not None:
                bs_att_id = row[0]
                text = text.replace(f'(../files/{target})', f'(/attachments/{bs_att_id})')

        # replace videos from vimeo; eg. [vimeo:123456789123456789:640:320]
        pos = 0
        while '[vimeo:' in text[pos:]:
            pos += text[pos:].index('[vimeo:') + len('[vimeo:')
            video = text[pos:]
            video = video[0:video.index(']')]
            video_id = video[0:video.index(':')]
            video = video[len(video_id)+1:]
            width = video[0:video.index(':')]
            video = video[len(width)+1:]
            height = video

            video_iframe = f'<iframe src="https://player.vimeo.com/video/{video_id}?title=0&amp;byline=0&amp;portrait=0&amp;color=8dc7dc" width="{width}" height="{height}" allowfullscreen="allowfullscreen"></iframe>'
            text = text.replace(f'[vimeo:{video_id}:{width}:{height}]', video_iframe)

        # replace https links in brackets with proper links eg. (https://vimeo.com/123456789123456789)
        pos = 0
        while ' (https://' in text[pos:]:
            pos += text[pos:].index(' (https://') + len(' (')
            link = text[pos:]
            link = link[0:link.index(')')]
            text = text.replace(f' ({link})', f' ([{link}]({link}))')


        # enter a space after # characters at start of line
        pos = 0
        r = re.search("\n#+[^\s^#]", text[pos:])
        while r:
            text = text[:pos + r.start()] + text[pos + r.start():pos + r.end() - 1] + ' ' + text[pos + r.end() - 1:]
            pos = r.end() + 1
            r = re.search("\n#+[^\s]", text[pos:])

        if not name:
            name = file_path.stem

        # calculate page slug from name
        page_slug = name.lower().replace(' ', '-')

        if not tags:
            # load tags from mysql database
            c=mydb.cursor()
            c.execute("""SELECT rt.family, rt.name FROM resource_tag AS rt
                JOIN resource_page_tag AS rpt on rpt.resource_tag_id = rt.id
                WHERE rpt.resource_page_id = %s""", (src_page_id,))
            row = c.fetchone()
            first_book_page_id = None
            tags = []
            while row is not None:
                tags.append({'name': row[0], 'value': row[1]})
                row = c.fetchone()

        return self.import_page_text(sq3, name, text, tags, book_id, chapter_id, src_page_id, page_id, book_slug, page_slug)


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
