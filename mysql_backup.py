__author__ = 'jglazner'
import argparse
from backup import MySQLDatabaseBackup

def _parse_command_line():
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


if __name__ == '__main__':
    args = _parse_command_line()
    backup = MySQLDatabaseBackup(args)
    backup.backup()