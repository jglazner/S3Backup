__author__ = 'jglazner'
from s3tools import parse_args
from s3tools.backup import S3BackupFileOrFolder, MySQLDatabaseBackup
import os

ROOT_FILE = "/".join([os.path.dirname(os.path.realpath(__file__)), 'resources/root_file.txt'])
RESOURCES_DIR = "/".join([os.path.dirname(os.path.realpath(__file__)), 'resources/'])
DEEPEST_CHILD = "/".join([os.path.dirname(os.path.realpath(__file__)), 'resources/subdir/childfile.txt'])

def setup():
    if not os.path.exists(RESOURCES_DIR):
        os.mkdir(RESOURCES_DIR)
    if not os.path.exists(ROOT_FILE):
        with open(ROOT_FILE, 'w') as f:
            f.write("root file")
    if not os.path.exists(RESOURCES_DIR + "subdir"):
        os.mkdir(RESOURCES_DIR + "subdir")
    if not os.path.exists(DEEPEST_CHILD):
        with open(DEEPEST_CHILD, 'w') as f:
            f.write("deepest child")

def test_calculate_s3_path():
    setup()
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'backup', 's3', ROOT_FILE, ])
    backup = S3BackupFileOrFolder(args)
    path, filename = backup.calculate_s3_path_for_file(ROOT_FILE)
    assert not path is None
    assert not filename is None
    assert path == os.path.dirname(os.path.realpath(ROOT_FILE))[1:] + "/"
    assert filename == os.path.basename(ROOT_FILE)

def test_backup_file_no_params():
    setup()
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'backup', 's3', ROOT_FILE])
    backup = S3BackupFileOrFolder(args)

    backup.bucket.delete_key(ROOT_FILE[1:])
    assert backup.bucket.get_key(ROOT_FILE[1:]) is None

    backup.backup_file(ROOT_FILE)
    assert not backup.bucket.get_key(ROOT_FILE[1:]) is None


def test_backup_file_with_path_param():
    setup()
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'backup', 's3', ROOT_FILE, 'test/backup'])
    backup = S3BackupFileOrFolder(args)
    expected = 'test/backup/root_file.txt'

    backup.bucket.delete_key(expected)
    assert backup.bucket.get_key(expected) is None

    backup.backup_file(ROOT_FILE, path=args.remote)
    assert not backup.bucket.get_key(expected) is None

def test_backup_file_with_all_params():
    setup()
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'backup', 's3', ROOT_FILE, 'test/backup', '--absolute-paths'])
    backup = S3BackupFileOrFolder(args)

    backup.bucket.delete_key(ROOT_FILE[1:])
    assert backup.bucket.get_key(ROOT_FILE[1:]) is None

    backup.backup_file(ROOT_FILE, path=args.remote)
    assert not backup.bucket.get_key(ROOT_FILE[1:]) is None

def test_backup_folder_no_params():
    setup()
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'backup', 's3', RESOURCES_DIR])
    backup = S3BackupFileOrFolder(args)
    backup.backup_folder()
    assert not backup.bucket.get_key(DEEPEST_CHILD[1:]) is None

def test_backup_folder_with_path():
    setup()
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'backup', 's3', RESOURCES_DIR, 'test/path'])
    assert args.remote == "test/path"
    backup = S3BackupFileOrFolder(args)
    backup.backup_folder()
    assert not backup.bucket.get_key('test/path/subdir/childfile.txt') is None

def test_backup_db():
    args = ['--profile', 'dennis', '-b', 'dennisestes-db-backups', 'backup', 'db', 'mtbseminars', '-u', 'mtbseminars', '-p', 'mtbseminars']
    parsed_args = parse_args(args=args)
    obj = MySQLDatabaseBackup(parsed_args)
    zipfile = obj.execute()
    assert not obj.bucket.get_key('mtbseminars/{0}/mtbseminars.sql.gz'.format(parsed_args.version)) is None
