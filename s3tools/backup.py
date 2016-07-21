__author__ = 'jglazner'
import os
import time
import tarfile
import shutil

from boto.s3.key import Key

from common import S3Base, MySQLBase


class S3BackupFileOrFolder(S3Base):
    def __init__(self, args):
        """
        Creates the S3Backup object.
        :param base_name: The prefix to use for the bucket.
        :return:
        """
        super(S3BackupFileOrFolder, self).__init__(args)
        self.file_or_folder = args.file_or_folder
        self.bucket = self.get_or_create_bucket("{0}".format(self.args.bucket))

    @staticmethod
    def get_file_paths(source_dir):
        file_paths = []
        for root, directories, filenames in os.walk(source_dir):
            for filename in filenames:
                file_paths.append(os.path.join(root, filename))
        return file_paths

    def backup_file(self, filename):
        print('Uploading %s to Amazon S3 bucket %s' % (filename, self.bucket.name))
        filesize = os.stat(filename).st_size
        if filesize > self.max_size:
            self.multipart_upload(self.bucket, filesize, filename)
        else:
            self.upload(self.bucket, filename)

    def backup_folder(self, source_dir):
        self.make_folder(self.bucket, source_dir)
        paths = self.get_file_paths(source_dir)
        counter = 0
        for filename in paths:
            self.backup_file(filename)
            counter += 1
        self.logger.info("Backed up {0} files under {1}".format(counter, source_dir))

    def execute(self):
        if os.path.isfile(self.file_or_folder):
            self.backup_file(self.file_or_folder)
        elif os.path.isdir(self.file_or_folder):
            self.backup_folder(self.file_or_folder)
        else:
            print("{0} was ignored because it's not a file or directory!".format(self.file_or_folder))


class MySQLDatabaseBackup(MySQLBase):
    def __init__(self, args):
        super(MySQLDatabaseBackup, self).__init__(args)

        self.bucket = self.get_or_create_bucket(self.args.bucket)
        if not self.bucket:
            raise RuntimeError("S3 Bucket {0} does not exit, and unable to create it!".format(self.args.bucket))

        if not self.version:
            self.version = time.strftime('%m%d%Y')
            path = "{0}/{1}".format(self.db_name, self.version)
            if not os.path.exists(path):
                os.makedirs(path)

    def execute(self):
        # Starting actual database backup process.
        self.logger.info("Starting backup of DB: {0} ...".format(self.args.db_name))
        dumpfile = self.backup()
        self.logger.info("Your dump has been created at {0}".format(dumpfile))

        # Now upload to S3
        self.logger.info("Starging upload to S3")
        self.upload("{0}/{1}".format(self.db_name, self.version), dumpfile)
        self.logger.info("Successfully uploaded to S3")
        if self.cleanup:
            shutil.rmtree(self.version)

