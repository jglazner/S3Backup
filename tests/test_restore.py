__author__ = 'jglazner'

from s3tools import parse_args
import shutil, os
from s3tools.restore import S3RestoreFileOrFolder, MySQLDatabaseRestore

ROOT_FILE = "/".join([os.path.dirname(os.path.realpath(__file__)), 'resources/root_file.txt'])
RESOURCES_DIR = "/".join([os.path.dirname(os.path.realpath(__file__)), 'resources/'])
DEEPEST_CHILD = "/".join([os.path.dirname(os.path.realpath(__file__)), 'resources/subdir/childfile.txt'])

def test_restore_file_no_args():
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'restore', 's3', ROOT_FILE[1:]])
    restore = S3RestoreFileOrFolder(args)
    if os.path.isfile(ROOT_FILE):
        os.unlink(ROOT_FILE)
    restore.execute()
    assert os.path.exists(ROOT_FILE)

def test_restore_folder_no_args():
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'restore', 's3', RESOURCES_DIR[1:]])
    restore = S3RestoreFileOrFolder(args)
    if os.path.exists(RESOURCES_DIR):
        shutil.rmtree(RESOURCES_DIR)
    restore.execute()
    assert os.path.exists(DEEPEST_CHILD) is True

def test_restore_db_version():
    args = ['--profile', 'dennis', '-b', 'dennisestes-db-backups', 'restore', 'db', 'mtbseminars', '-u', 'mtbseminars', '-p', 'mtbseminars', '-v', '07222016']
    parsed_args = parse_args(args=args)
    obj = MySQLDatabaseRestore(parsed_args)
    obj.execute()
    assert not obj.bucket.get_key('mtbseminars/{0}/mtbseminars.sql.gz'.format(parsed_args.version)) is None
    