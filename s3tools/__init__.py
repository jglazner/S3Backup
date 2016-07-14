__author__ = 'jglazner'

import argparse
import logging

import boto
from boto.exception import S3ResponseError
from abc import ABCMeta, abstractmethod

from s3tools.backup import MySQLDatabaseBackup, S3Backup
from s3tools.restore import MySQLDatabaseRestore, S3Restore


class S3ToolsCommand(object):

    __metaclass__ = ABCMeta

    def __init__(self, args):
        self.args = args
        self.log_file='s3.log'


    @abstractmethod
    def execute(self):
        pass

class S3Base(S3ToolsCommand):
    def __init__(self, args):
        super(S3Base).__init__(args)
        self.conn = boto.connect_s3()
        self.logger = configure_logging(self.__class__.__name__, self.log_file)

    def get_or_create_bucket(self, bucket_name):
        bucket = None
        try:
            bucket = self.conn.get_bucket(bucket_name)
        except S3ResponseError:
            bucket = self.conn.create_bucket(bucket_name)
            bucket.set_canned_acl('private')

        return bucket

    @abstractmethod
    def execute(self):
        pass


def configure_logging(name, log_file):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


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

    restore = subparsers.add_parser('backup', help='Restore Commands')
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