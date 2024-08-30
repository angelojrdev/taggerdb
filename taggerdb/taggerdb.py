#!/usr/bin/env python3

import os
import sqlite3
import hashlib
import argparse


def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            size INTEGER NOT NULL
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS file_tags (
            file_id INTEGER,
            tag_id INTEGER,
            PRIMARY KEY (file_id, tag_id),
            FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
            FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
        )
    """
    )

    conn.commit()


def scan_directory_and_insert(conn, dir_path):
    cursor = conn.cursor()
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, dir_path)
            file_size = os.path.getsize(file_path)

            hash_obj = hashlib.new("sha256")
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_obj.update(chunk)
            file_sha256 = hash_obj.hexdigest()

            cursor.execute(
                """
                SELECT 1 FROM files WHERE sha256 = ? AND size = ?
            """,
                (file_sha256, file_size),
            )
            if cursor.fetchone() is None:
                cursor.execute(
                    """
                    INSERT INTO files (location, sha256, size)
                    VALUES (?, ?, ?)
                """,
                    (relative_path, file_sha256, file_size),
                )
                print(f'Info: Added File "{relative_path}"')

    conn.commit()


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
        choices=["init", "scan"],
        help="Action to peform",
    )

    parser.add_argument(
        "--directory",
        "-d",
        type=str,
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

    arguments = parser.parse_args()

    if arguments.action == "scan":
        if arguments.directory is None:
            print("Error: --directory is required for 'scan' action.")
            exit(1)

    return arguments


def main():
    arguments = parse_arguments()

    match arguments.action:
        case "init":
            if os.path.exists(arguments.database):
                print(f"Error: {arguments.database} already exists.")
                exit(1)

            conn = sqlite3.connect(arguments.database)
            create_tables(conn)
            conn.close()
            print("Info: Database was initialized successfully!")
        case "scan":
            if not os.path.exists(arguments.database):
                print(f"Error: {arguments.database} doesn't exist.")
                exit(1)

            conn = sqlite3.connect(arguments.database)
            scan_directory_and_insert(conn, arguments.directory)
            print("Info: Done!")
            conn.close()

    # if arguments.tags:
    #     cursor = conn.cursor()
    #     cursor.execute("SELECT id FROM files")
    #     files = cursor.fetchall()

    #     for file_id in files:
    #         add_tags(conn, file_id[0], arguments.tags)


if __name__ == "__main__":
    main()
