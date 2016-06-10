__author__ = 'jglazner'
import boto
import os
import time
import tarfile
import logging
import shutil
import getpass
from boto.s3.key import Key
from boto.exception import S3ResponseError


def configure_logging(name, log_file):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


class S3Base(object):
    def __init__(self, log_file='s3.log'):
        self.conn = boto.connect_s3()
        self.logger = configure_logging('S3Backup', log_file)
        self.bucket = None

    def get_or_create_bucket(self, bucket_name):
        bucket = None
        try:
            bucket = self.conn.get_bucket(bucket_name)
        except S3ResponseError:
            bucket = self.conn.create_bucket(bucket_name)
            bucket.set_canned_acl('private')

        return bucket


class S3Backup(S3Base):
    def __init__(self, name):
        """
        Creates the S3Backup object.
        :param base_name: The prefix to use for the bucket.
        :return:
        """
        super(S3Backup, self).__init__(log_file='s3backup.log')
        assert name is not None
        self.name = name
        self.max_size = 20 * 1000 * 1000
        self.part_size = 6 * 1000 * 1000
        self.bucket = self.get_or_create_bucket("{0}".format(self.name))

    def percent_cb(self, so_far, total):
        self.logger.info("complete={0}, total={0}".format(so_far, total))

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
        self.logger.info("Using multi-part upload for %s" % filename)
        mp = self.bucket.initiate_multipart_upload(filename)
        fp = open(filename, 'rb')
        fp_num = 0
        while fp.tell() < filesize:
            fp_num += 1
            self.logger.info("Uploading part %i of %s" % (fp_num, filename))
            mp.upload_part_from_file(fp, fp_num, cb=self.percent_cb, num_cb=10, size=self.part_size)

            mp.complete_upload()

    def upload(self, filename):
        self.logger.debug("Using single upload for %s" % filename)
        k = boto.s3.key.Key(self.bucket)
        k.key = filename
        k.set_contents_from_filename(filename, cb=self.percent_cb, num_cb=10)

    def backup_file(self, filename):
        self.logger.info('Uploading %s to Amazon S3 bucket %s' % (filename, self.bucket.name))
        filesize = os.path.getsize(filename)
        if filesize > self.max_size:
            self.multipart_upload(filesize, filename)
        else:
            self.upload(filename)

    def backup_folder(self, source_dir):
        self.make_folder(source_dir)
        paths = self.get_file_paths(source_dir)
        for filename in paths:
            self.backup_file(filename)

    def backup(self, files_and_folders):
        for path in files_and_folders:
            if os.path.isfile(path):
                self.backup_file(path)
            elif os.path.isdir(path):
                self.backup_folder(path)
            else:
                self.logger.error("{0} was ignored because it's not a file or directory!".format(path))


class S3Restore(S3Base):
    def __init__(self, bucket_name):
        super(S3Restore, self).__init__("s3restore.log")
        self.bucket = self.conn.get_bucket(bucket_name)

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


class MySQLDatabaseBackup(object):
    def __init__(self, args):
        self.logger = configure_logging('DBBackup', 'db_backup.log')
        self.args = args

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

        if self.args.clean:
            shutil.rmtree(backup_path)

