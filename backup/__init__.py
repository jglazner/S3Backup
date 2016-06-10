__author__ = 'jglazner'
import argparse
from backup import MySQLDatabaseBackup
from backup import S3Backup

def parse_s3_backup_args(subparser):
    s3 = subparser.add_parser('s3', help='Data Commands')
    s3.set_defaults(command=s3backup)
    s3.add_argument('FILE_OR_DIRECTORY', help="The file or folder to backup to the specified s3 bucket")
    s3.add_argument(
        '-b',
        '--bucket',
        required=True,
        help='Bucket name'
    )


def parse_db_backup_args(subparser):
    db = subparser.add_parser('db', help='Database Backup Options')
    db.set_defaults(command=dbbackup)
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
    root_parser = argparse.ArgumentParser(prog="backup")
    subparsers = root_parser.add_subparsers(help='Available backup:')
    parse_db_backup_args(subparsers)
    parse_s3_backup_args(subparsers)
    return root_parser.parse_args()

def main():
    args = parse_args()
    args.command(args)


def dbbackup(args):
    db_backup = MySQLDatabaseBackup(args)
    db_backup.backup()


def s3backup(args):
    s3_backup = S3Backup(args.bucket)
    s3_backup.backup(args.FILE_OR_DIRECTORY)