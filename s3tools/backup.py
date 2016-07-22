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
        self.bucket = self.get_or_create_bucket("{0}".format(self.args.bucket))
        self.use_absolute_paths = args.absolute_paths
        self.local = args.local
        self.remote = args.remote
        if not self.remote and not self.use_absolute_paths:
            self.logger.warning("No remote location specified, assuming absolute paths!")
            self.use_absolute_paths = True


    @staticmethod
    def get_file_paths_to_backup(source_dir):
        if not os.path.isdir(source_dir):
            raise IOError("Source directory '{0}' does not exist".format(source_dir))
        file_paths = []
        for root, directories, filenames in os.walk(source_dir):
            for filename in filenames:
                file_paths.append(os.path.join(root, filename))
        return file_paths

    def backup_file(self, filename, path=None):
        if self.use_absolute_paths:
            path = os.path.dirname(os.path.realpath(filename)) + "/"
        elif self.remote:
            path = self.remote

        print('Uploading %s to Amazon S3 bucket %s' % (filename, self.bucket.name))
        filesize = os.stat(self.local).st_size
        if filesize > self.max_size:
            self.multipart_upload(self.bucket, filesize, filename, path)
        else:
            with open(filename) as f:
                self.upload(self.bucket, f, path)

    def calculate_s3_path_for_file(self, filename):
        local_path = os.path.dirname(os.path.realpath(filename)) + "/"
        filename = os.path.basename(filename)
        if self.use_absolute_paths:
            return local_path[1:], filename
        else:
            if os.path.isdir(self.local):
                nested_local_dirs = local_path.replace(os.path.realpath(self.local), "")
                s3_path = self.remote + nested_local_dirs
            else:
                s3_path = self.remote
            return s3_path, filename

    def backup_folder(self):
        paths = self.get_file_paths_to_backup(self.local)
        counter = 0
        for filename in paths:
            path, s3_filename = self.calculate_s3_path_for_file(filename)
            self.backup_file(filename, path=path)
            counter += 1
        self.logger.info("Backed up {0} files to S3:{1}".format(counter, self.remote))

    def execute(self):
        if os.path.isfile(self.local):
            self.backup_file(self.local)
        elif os.path.isdir(self.local):
            self.backup_folder()
        else:
            print("{0} was ignored because it's not a file or directory!".format(self.local))


class MySQLDatabaseBackup(MySQLBase):
    def __init__(self, args):
        super(MySQLDatabaseBackup, self).__init__(args)

        self.bucket = self.get_or_create_bucket(self.args.bucket)
        if not self.bucket:
            raise RuntimeError("S3 Bucket {0} does not exit, and unable to create it!".format(self.args.bucket))

        path = "{0}/{1}".format(self.db_name, self.version)
        if not os.path.exists(path):
            os.makedirs(path)

    def execute(self):
        # Starting actual database backup process.
        self.logger.info("Starting backup of DB: {0} ...".format(self.db_name))
        dumpfile = self.backup()
        self.logger.info("Your dump has been created at {0}".format(dumpfile))

        # Now upload to S3
        self.logger.info("Starging upload to S3")
        with open(dumpfile) as f:
            self.upload(self.bucket, f, "{0}/{1}".format(self.db_name, self.version))
        self.logger.info("Successfully uploaded to S3")
        if self.cleanup:
            shutil.rmtree(self.version)

