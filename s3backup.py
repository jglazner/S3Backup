__author__ = 'jglazner'
import argparse
from backup import S3Backup
from config import AWSConfig

def _parse_command_line():
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


if __name__ == '__main__':
    args = _parse_command_line()
    backup = S3Backup(args.bucket)
    if args.file:
        backup.backup_file(args.file)
    elif args.dir:
        backup.backup_folder(args.dir)
    elif args.multi:
        files_and_folders = args.multi.split(",")
        backup.backup(files_and_folders)




