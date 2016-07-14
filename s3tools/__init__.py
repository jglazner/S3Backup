__author__ = 'jglazner'

import argparse
import logging

from s3tools.backup import MySQLDatabaseBackup, S3Backup
from s3tools.restore import MySQLDatabaseRestore, S3Restore


def parse_s3_args(subparser, command):
    s3 = subparser.add_parser('s3', help='S3 Backup/Restore Commands')
    s3.set_defaults(command=command)
    s3.add_argument('FILE_OR_DIRECTORY', help="The file or folder to backup/restore to/from the specified s3 bucket")
    s3.add_argument(
        '-b',
        '--bucket',
        required=True,
        help='Bucket name'
    )


def parse_db_args(subparser, command):
    db = subparser.add_parser('db', help='Database Backup/Restore Options')
    db.set_defaults(command=command)
    db.add_argument(
        '-n',
        '--name',
        required=True,
        help="Name of the MySQL Database"
    )
    db.add_argument(
        '-u',
        '--username',
        required=True,
        help="MySQL Username"
    )
    db.add_argument(
        '-p',
        '--password',
        help="MySQL Password"
    )
    db.add_argument(
        '-b',
        '--bucket',
        required=True,
        help="Name of the S3Bucket to back up to"
    )
    db.add_argument(
        '-c',
        '--cleanup',
        help="Remove local backup files after successfully uploading to S3"
    )

def parse_args():
    root_parser = argparse.ArgumentParser(prog="s3tools")
    subparsers = root_parser.add_subparsers(help='S3Tools help:')

    backup = subparsers.add_parser('backup', help='Backup Commands')
    backup_parser = backup.add_subparsers(help="Backup Commands")
    parse_db_args(backup_parser, db_backup)
    parse_s3_args(backup_parser, s3_backup)

    restore = subparsers.add_parser('restore', help='Restore Commands')
    restore_parser = restore.add_subparsers(help="Restore Commands")
    parse_db_args(restore_parser, db_restore)
    parse_s3_args(restore_parser, s3_restore)

    return root_parser.parse_args()


def main():
    args = parse_args()
    args.command(args)


def db_restore(args):
    restore = MySQLDatabaseRestore(args)
    restore.restore()


def db_backup(args):
    backup = MySQLDatabaseBackup(args)
    backup.backup()


def s3_restore(args):
    restore = S3Restore(args)
    restore.restore()


def s3_backup(args):
    backup = S3Backup(args)
    backup.restore()