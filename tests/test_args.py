__author__ = 'jglazner'
from s3tools import parse_args
import inspect
import time

def test_s3_backup_args():
    args = parse_args(args=['--profile', 'mickey', '-b', 'mybucket', 'backup', 's3', 'local', 'remote'])
    assert args.profile == "mickey"
    assert args.bucket == "mybucket"
    assert inspect.isfunction(args.command) is True
    assert args.command.__name__ == "s3_backup"
    assert args.local == "local"
    assert args.remote == "remote"
    assert args.absolute_paths is False

def test_mysql_args():
    args = ['--profile', 'mickey', '-b', 'mybucket', 'backup', 'db', 'test', '-u', 'uname', '-p', 'password']
    parsed_args = parse_args(args=args)
    assert parsed_args.profile == "mickey"
    assert parsed_args.bucket == "mybucket"
    assert inspect.isfunction(parsed_args.command) is True
    assert parsed_args.command.__name__ == "db_backup"
    assert parsed_args.name == "test"
    assert parsed_args.username == "uname"
    assert parsed_args.password == "password"
    assert parsed_args.cleanup is False
    assert parsed_args.version == time.strftime('%m%d%Y')