__author__ = 'jglazner'

from s3tools import parse_args
import shutil, os
from s3tools.restore import S3RestoreFileOrFolder

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

def test_backup_folder():
    """
    args = parse_args(args=['--profile', 'dennis', '-b', 'dennisestes-unittests', 'restore', 's3', RESOURCES_DIR[1:]])
    restore = S3RestoreFileOrFolder(args)
    if os.path.exists(RESOURCES_DIR):
        shutil.rmtree(RESOURCES_DIR)
    restore.execute()
    if not os.path.exists(DEEPEST_CHILD):
        raise RuntimeError("Barf!")
    """
    pass

test_restore_file_no_args()