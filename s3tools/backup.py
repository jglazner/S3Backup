__author__ = 'jglazner'
import boto
import os
import time
import tarfile
import shutil
import getpass
import math
from filechunkio import FileChunkIO
from boto.s3.key import Key

from common import S3Base


class S3Backup(S3Base):
    def __init__(self, args):
        """
        Creates the S3Backup object.
        :param base_name: The prefix to use for the bucket.
        :return:
        """
        super(S3Backup, self).__init__(args)
        assert args.name is not None
        self.name = args.name
        self.max_size = 20 * 1000 * 1000
        self.part_size = 6 * 1000 * 1000
        self.bucket = self.get_or_create_bucket("{0}".format(self.name))

    @staticmethod
    def get_file_paths(source_dir):
        file_paths = []
        for root, directories, filenames in os.walk(source_dir):
            for filename in filenames:
                file_paths.append(os.path.join(root, filename))
        return file_paths

    def make_folder(self, name):
        if not name.endswith("/"):
            name += "/"
        # if the folder already exists, just return it.
        folder = self.bucket.get_key(name)
        if folder:
            return folder

        progress = ""
        for part in name.split("/"):
            progress += part + "/"
            if not self.bucket.get_key(progress):
                key = self.bucket.new_key(progress)
                key.set_contents_from_string('')
                key.set_canned_acl('private')

        return self.bucket.get_key(name)

    def multipart_upload(self, filesize, filename):
        print("Using multi-part upload for {0}...".format(filename))
        mp = self.bucket.initiate_multipart_upload(filename)
        chunk_count = int(math.ceil(filesize / float(self.part_size)))

        for i in range(chunk_count):
            offset = self.part_size * i
            bytes = min(self.part_size, filesize - offset)
            with FileChunkIO(filename, 'r', offset=offset, bytes=bytes) as fp:
                mp.upload_part_from_file(fp, part_num=i + 1)

        print("Upload complete.")
        mp.complete_upload()

    def upload(self, filename):
        k = boto.s3.key.Key(self.bucket)
        k.key = filename
        k.set_contents_from_filename(filename)

    def backup_file(self, filename):
        print('Uploading %s to Amazon S3 bucket %s' % (filename, self.bucket.name))
        filesize = os.stat(filename).st_size
        if filesize > self.max_size:
            self.multipart_upload(filesize, filename)
        else:
            self.upload(filename)

    def backup_folder(self, source_dir):
        self.make_folder(source_dir)
        paths = self.get_file_paths(source_dir)
        counter = 0
        for filename in paths:
            self.backup_file(filename)
            counter += 1
        self.logger.info("Backed up {0} files under {1}".format(counter, source_dir))

    def backup(self, path):
        if os.path.isfile(path):
            self.backup_file(path)
        elif os.path.isdir(path):
            self.backup_folder(path)
        else:
            print("{0} was ignored because it's not a file or directory!".format(path))


class S3Restore(S3Base):
    def __init__(self, args):
        super(S3Restore, self).__init__(args)
        self.name = args.name
        self.bucket = self.get_or_create_bucket("{0}".format(self.name))

    def restore_all(self):
        keys = self.bucket.get_all_keys()
        for key in keys:
            folder = "/{0}".format('/'.join(key.name.split('/')[0:-1]))
            if not os.path.isdir(folder):
                os.makedirs(folder)
            downloaded = key.get_contents_to_filename("/{0}".format(key.name))

    def print_keys(self):
        keys = self.bucket.get_all_keys()
        print keys


class MySQLDatabaseBackup(S3Base):
    def __init__(self, args):
        super(MySQLDatabaseBackup, self).__init__(args)

    def make_tarfile(self, output_dir, source_file):
            output = "{0}/{1}.tar.gz".format(output_dir, self.args.name)
            with tarfile.open(output, "w:gz") as tar:
                tar.add(source_file, arcname=os.path.basename(source_file))

            return output

    def upload(self, folder, tarball):

        self.logger.info("Uploading dump to S3 ...")
        s3_backup = S3Backup(self.args.bucket)
        s3_backup.make_folder(folder)
        s3_backup.backup_file(tarball)
        self.logger.info("Database backup has been uploaded successfully to S3.")

    def backup(self):
        today = time.strftime('%m%d%Y-%H%M%S')
        backup_path = "{0}/{1}".format(self.args.name, today)
        if not os.path.exists(backup_path):
            os.makedirs(backup_path)
        # Starting actual database backup process.
        self.logger.info("Starting backup of DB: {0} ...".format(self.args.name))

        if self.args.password:
            password = self.args.password
        else:
            password = getpass.getpass()
        cmd = "mysqldump -u {0} -p{1} {2} > {3}/{4}.sql".format(
            self.args.username, password, self.args.name, backup_path, self.args.name)
        os.system(cmd)
        dumpfile = "{0}/{1}.sql".format(backup_path,self.args.name)
        self.logger.info("Database dump completed.")

        self.logger.info("Compressing...")
        tarball = self.make_tarfile(backup_path, dumpfile)
        self.logger.info("Your dump has been created at {0}".format(tarball))
        self.upload(backup_path, tarball)
        self.logger.info("Successfully uploaded to S3")
        if self.args.cleanup:
            shutil.rmtree(backup_path)

