#!/usr/bin/env python3

import os
import sqlite3
import hashlib
import argparse


def calculate_sha256(file_path):
    hash_obj = hashlib.new("sha256")
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def get_file_size(file_path):
    return os.path.getsize(file_path)


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
            file_sha256 = calculate_sha256(file_path)
            file_size = get_file_size(file_path)

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
                print(f"Added File: {relative_path}")

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


def main():
    parser = argparse.ArgumentParser(
        description=(
            "TaggerDB - A user-friendly tool designed for managing and tagging files. "
            "Ideal for developers, researchers, and anyone needing to organize files "
            "using customizable tags."
        )
    )

    parser.add_argument(
        "--dir", "-d", type=str, required=True, help="Storage directory, required"
    )

    parser.add_argument(
        "--db", "-b", type=str, default="tagger.db3", help="Database file, default=tagger.db3"
    )

    parser.add_argument("--tags", nargs="+", help="Tags to associate with files")

    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    create_tables(conn)

    scan_directory_and_insert(conn, args.dir)

    if args.tags:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM files")
        files = cursor.fetchall()

        for file_id in files:
            add_tags(conn, file_id[0], args.tags)

    conn.close()


if __name__ == "__main__":
    main()
