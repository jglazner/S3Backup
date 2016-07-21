__author__ = 'jglazner'

import os
from common import S3Base, MySQLBase
from sortedcontainers import SortedList


class S3RestoreFileOrFolder(S3Base):
    def __init__(self, args):
        super(S3RestoreFileOrFolder, self).__init__(args)
        if args.file_or_folder.startswith("/"):
            self.file_or_folder = args.file_or_folder[1:]
        else:
            self.file_or_folder = args.file_or_folder
        self.bucket = self.get_bucket(self.args.bucket)
        if not self.bucket:
            raise RuntimeError("S3 Bucket {0} does not exit!".format(self.args.bucket))

    def execute(self):
        keys = self.bucket.list(prefix=self.file_or_folder)
        for key in keys:
            self.download(self.bucket, key.name)


class MySQLDatabaseRestore(MySQLBase):
    def __init__(self, args):
        super(MySQLDatabaseRestore, self).__init__(args)
        self.bucket = self.get_bucket(self.args.bucket)
        if not self.bucket:
            raise RuntimeError("S3 Bucket {0} does not exit!".format(self.args.bucket))

        if not self.version:
            self.version = self.find_latest()

    def find_latest(self):
        sorted = SortedList()
        for i in self.bucket.list(prefix=self.db_name):
            sorted.add(i.name)
        return sorted[len(sorted)-1]

    def execute(self):
        db_backup = self.bucket.get_key("{1}/{0}".format(self.version, self.db_name))
        tarfile = self.download(db_backup)
        self.restore()



