__author__ = 'jglazner'
from s3tools import parse_args
from s3tools.backup import S3BackupFileOrFolder
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

def test_backup_file():
    setup()
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'backup', 's3', ROOT_FILE])
    backup = S3BackupFileOrFolder(args)
    backup.backup_file(ROOT_FILE)

def test_backup_folder():
    setup()
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'backup', 's3', RESOURCES_DIR])
    backup = S3BackupFileOrFolder(args)
    backup.backup_folder(RESOURCES_DIR)
    assert not backup.bucket.get_key(DEEPEST_CHILD) is None

