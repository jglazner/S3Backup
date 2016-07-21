__author__ = 'jglazner'
from s3tools import parse_args
import inspect

def test_s3_backup_args():
    args = parse_args(args=['--profile', 'mickey', '--b', 'mybucket', 'backup', 's3', 'filename'])
    assert args.profile == "mickey"
    assert args.bucket == "mybucket"
    assert inspect.isfunction(args.command) is True
    assert args.command.__name__ == "s3_backup"
    assert args.file_or_folder == "filename"