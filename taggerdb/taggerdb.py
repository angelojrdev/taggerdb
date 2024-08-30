#!/usr/bin/env python3

import os
import sqlite3
import hashlib
import argparse


class TaggerDb:
    def __init__(self, database, storage, quickstart=False):
        self.database = database
        self.storage = storage

        if quickstart:
            self.connect()
            self.prepare_database()

    def connect(self):
        try:
            if self.is_connected():
                raise Exception("Database is connected already.")

            self.connection = sqlite3.connect(self.database)
            print("Info: Database connected.")
        except Exception as e:
            print(f"Error: {e}")

    def disconnect(self):
        try:
            if not self.is_connected():
                raise Exception("Database isn't connected.")

            self.connection.close()
            print("Info: Database disconnected.")
        except Exception as e:
            print(f"Error: {e}")

    def is_connected(self):
        try:
            self.connection.execute("SELECT 1")
            return True
        except Exception:
            return False

    def prepare_database(self):
        try:
            if not self.is_connected():
                raise Exception("Database isn't connected.")

            cursor = self.connection.cursor()

            cursor.execute(
                """ CREATE TABLE IF NOT EXISTS file (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    location TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size INTEGER NOT NULL )"""
            )

            cursor.execute(
                """ CREATE TABLE IF NOT EXISTS tag (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE )"""
            )

            cursor.execute(
                """ CREATE TABLE IF NOT EXISTS tag_x_file (
                    file INTEGER,
                    tag INTEGER,
                    PRIMARY KEY (file, tag),
                    FOREIGN KEY (file) REFERENCES file(id) ON DELETE CASCADE,
                    FOREIGN KEY (tag) REFERENCES tag(id) ON DELETE CASCADE )"""
            )

            self.connection.commit()
            print("Info: Database is now ready.")
        except Exception as e:
            print(f"Error: {e}")

    def scan(self):
        cursor = self.connection.cursor()
        for root, _, files in os.walk(self.storage):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, self.storage)
                file_size = os.path.getsize(file_path)

                hash_obj = hashlib.new("sha256")
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_obj.update(chunk)
                file_sha256 = hash_obj.hexdigest()

                cursor.execute(
                    "SELECT 1 FROM file WHERE sha256 = ? AND size = ?",
                    (file_sha256, file_size),
                )

                if cursor.fetchone() is not None:
                    continue

                cursor.execute(
                    """ INSERT INTO file (location, sha256, size)
                    VALUES (?, ?, ?) """,
                    (relative_path, file_sha256, file_size),
                )
                print(f'Info: Added File "{relative_path}"')

        self.connection.commit()
        print("Info: Done!")


def add_tags(conn, file_id, tags):
    cursor = conn.cursor()

    for tag_name in tags:
        cursor.execute(
            """
            INSERT OR IGNORE INTO tags (name)
            VALUES (?)
        """,
            (tag_name,),
        )

        cursor.execute(
            """
            SELECT id FROM tags WHERE name = ?
        """,
            (tag_name,),
        )
        tag_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT OR IGNORE INTO file_tags (file_id, tag_id)
            VALUES (?, ?)
        """,
            (file_id, tag_id),
        )

    conn.commit()


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog="taggerdb",
        description=(
            "TaggerDB - A user-friendly tool designed for managing and tagging files. "
            "Ideal for developers, researchers, and anyone needing to organize files "
            "using customizable tags."
        ),
    )

    parser.add_argument(
        "action",
        choices=["scan"],
        help="Action to peform",
    )

    parser.add_argument(
        "--directory",
        "-d",
        type=str,
        required=True,
        help="Storage directory",
    )

    parser.add_argument(
        "--database",
        "-b",
        type=str,
        default="tagger.db3",
        help="Database file",
    )

    parser.add_argument("--tags", nargs="+", help="Tags to associate with files")

    return parser.parse_args()


def main():
    arguments = parse_arguments()
    taggerdb = TaggerDb(arguments.database, arguments.directory, True)

    match arguments.action:
        case "scan":
            taggerdb.scan()
    
    taggerdb.disconnect()

    # if arguments.tags:
    #     cursor = conn.cursor()
    #     cursor.execute("SELECT id FROM files")
    #     files = cursor.fetchall()

    #     for file_id in files:
    #         add_tags(conn, file_id[0], arguments.tags)


if __name__ == "__main__":
    main()
