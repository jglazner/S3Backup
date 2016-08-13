__author__ = 'jglazner'

import argparse

from s3tools.backup import MySQLDatabaseBackup, S3BackupFileOrFolder
from s3tools.restore import MySQLDatabaseRestore, S3RestoreFileOrFolder


def parse_s3_backup_args(subparser, command):
    s3 = subparser.add_parser('s3', help='S3 Backup/Restore Commands')
    s3.set_defaults(command=command)
    s3.add_argument(
        'local',
        help="The local file or folder to backup/restore to/from the specified s3 bucket"
    )
    s3.add_argument(
        'remote',
        nargs='?',
        default=None,
        help="The remote file or folder to backup/restore to/from the specified s3 bucket"
    )
    s3.add_argument(
        '-a',
        '--absolute-paths',
        action="store_true",
        help='The base remote path in S3 to copy the files to'
    )

def parse_s3_restore_args(subparser, command):
    s3 = subparser.add_parser('s3', help='S3 Backup/Restore Commands')
    s3.set_defaults(command=command)
    s3.add_argument(
        'remote',
        help="The remote file or folder to backup/restore to/from the specified s3 bucket"
    )
    s3.add_argument(
        'local',
        nargs='?',
        help="The local file or folder to backup or restore to or from the specified s3 bucket"
    )
    s3.add_argument(
        '-a',
        '--absolute-paths',
        action="store_true",
        help='The base remote path in S3 to copy the files to'
    )


def parse_db_args(subparser, command):
    db = subparser.add_parser('db', help='Database Backup/Restore Options')
    db.set_defaults(command=command)
    db.add_argument(
        'name',
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
        '--cleanup',
        action='store_true',
        help="Remove local backup files after successfully uploading to S3"
    )
    db.add_argument(
        '-v',
        '--version',
        default=None,
        help="The version of the DB you are backing up or restoring. During a RESTORE, if this is not specified, it will attempt to find the most recent backup. During a BACKUP, if this is not specified it will default to today's date in '%m%d%Y' format."
    )

def parse_args(args=None):
    root_parser = argparse.ArgumentParser(prog="s3tools")
    root_parser.add_argument(
        '--profile',
        default="default",
        help="Which aws profile to use from .aws/credentials. Default=default"
    )
    root_parser.add_argument(
        '-b',
        '--bucket',
        required=True,
        help='Bucket name'
    )

    subparsers = root_parser.add_subparsers(help='S3Tools help:')

    backup = subparsers.add_parser('backup', help='Backup Commands')
    backup_parser = backup.add_subparsers(help="Backup Commands")
    parse_db_args(backup_parser, db_backup)
    parse_s3_backup_args(backup_parser, s3_backup)

    restore = subparsers.add_parser('restore', help='Restore Commands')
    restore_parser = restore.add_subparsers(help="Restore Commands")
    parse_db_args(restore_parser, db_restore)
    parse_s3_restore_args(restore_parser, s3_restore)

    return root_parser.parse_args(args=args)


def main():
    args = parse_args()
    args.command(args)


def db_restore(args):
    restore = MySQLDatabaseRestore(args)
    restore.execute()


def db_backup(args):
    backup = MySQLDatabaseBackup(args)
    backup.execute()


def s3_restore(args):
    restore = S3RestoreFileOrFolder(args)
    restore.execute()


def s3_backup(args):
    backup = S3BackupFileOrFolder(args)
    backup.execute()