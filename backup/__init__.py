__author__ = 'jglazner'
import argparse
from backup import MySQLDatabaseBackup
from backup import S3Backup

def parse_s3_backup_args():
    parser = argparse.ArgumentParser(
        description='Name of the S3 bucket to backup')
    parser.add_argument(
        '-b',
        '--bucket',
        help='Bucket name'
    )
    parser.add_argument(
        '-f',
        '--file',
        help="Backup a specific file"
    )
    parser.add_argument(
        '-d',
        '--directory',
        help="Backup an entire folder"
    )
    parser.add_argument(
        '-m',
        '--multi',
        help="Backup a list of files or folders"
    )
    return parser.parse_args()


def parse_db_backup_args():
    parser = argparse.ArgumentParser(
        description='Back up a MySQL DB to S3')
    parser.add_argument(
        '-n',
        '--name',
        help="Name of the MySQL Database"
    )
    parser.add_argument(
        'u',
        '--username',
        required=True,
        help="MySQL Username"
    )
    parser.add_argument(
        'p',
        '--password',
        help="MySQL Password"
    )
    parser.add_argument(
        'b',
        '--bucket',
        required=True,
        help="Name of the S3Bucket to back up to"
    )
    parser.add_argument(
        'c',
        '--cleanup',
        help="Remove local backup files after successfully uploading to S3"
    )
    return parser.parse_args()


def dbbackup():
    args = parse_db_backup_args()
    backup = MySQLDatabaseBackup(args)
    backup.backup()

def s3backup():
    args = parse_s3_backup_args()
    backup = S3Backup(args.bucket)
    if args.file:
        backup.backup_file(args.file)
    elif args.dir:
        backup.backup_folder(args.dir)
    elif args.multi:
        files_and_folders = args.multi.split(",")
        backup.backup(files_and_folders)