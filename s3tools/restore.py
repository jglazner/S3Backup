__author__ = 'jglazner'

from common import S3Base


class S3Restore(S3Base):
    def __init__(self, args):
        super(S3Restore, self).__init__(args)

    def restore(self):
        print("Not Implemented")


class MySQLDatabaseRestore(S3Base):
    def __init__(self, args):
        super(MySQLDatabaseRestore, self).__init__(args)

    def restore(self):
        print("Not Implemented")