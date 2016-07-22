__author__ = 'jglazner'

import os
from common import S3Base, MySQLBase
from sortedcontainers import SortedList


class S3RestoreFileOrFolder(S3Base):
    def __init__(self, args):
        super(S3RestoreFileOrFolder, self).__init__(args)
        self.use_absolute_paths = args.absolute_paths
        self.local = args.local
        self.remote = args.remote
        self.bucket = self.get_bucket(self.args.bucket)
        if not self.bucket:
            raise RuntimeError("S3 Bucket {0} does not exit!".format(self.args.bucket))

        if not self.local:
            self.logger.warning("No local path specified to download to, assuming absolute paths!")
            self.use_absolute_paths = True

    def execute(self):
        keys = self.bucket.list(prefix=self.remote)
        for key in keys:
            if self.use_absolute_paths:
                local = "/" + key.name
            else:
                local = self.local
            self.download(key, local)


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
        self.logger.info("Downloading backup of DB: {0} ...".format(self.db_name))
        db_backup = self.bucket.get_key(self.gzipfile)
        if db_backup:
            self.download(db_backup, "{0}/{1}".format(os.getcwd(), self.gzipfile))
            self.logger.info("Download complete.".format(self.db_name))

            self.logger.info("Restoring...")
            self.restore()
            self.logger.info("Restored!")
        else:
            raise IOError("Backup file: {0} could not be found in S3 bucket: {1}!".format(self.gzipfile, self.bucket.name))


