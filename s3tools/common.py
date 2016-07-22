__author__ = 'jglazner'

import os
import boto
import logging
import getpass
import math
from filechunkio import FileChunkIO
from boto.exception import S3ResponseError
from abc import ABCMeta, abstractmethod


class S3ToolsCommand(object):

    __metaclass__ = ABCMeta

    def __init__(self, args):
        self.args = args
        self.logger = self.configure_logging()

    @abstractmethod
    def execute(self):
        pass

    def configure_logging(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh = logging.FileHandler('s3.log')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        return logger


class S3Base(S3ToolsCommand):
    def __init__(self, args):
        super(S3Base, self).__init__(args)
        self.conn = boto.connect_s3(profile_name=args.profile)
        self.max_size = 20 * 1000 * 1000
        self.part_size = 6 * 1000 * 1000

    def get_bucket(self, bucket_name):
        try:
            return self.conn.get_bucket(bucket_name)
        except S3ResponseError:
            return None

    def get_or_create_bucket(self, bucket_name):

        bucket = self.get_bucket(bucket_name)
        if bucket:
            return bucket
        else:
            bucket = self.conn.create_bucket(bucket_name)
            bucket.set_canned_acl('private')

        return bucket

    def download(self, remote, local):
        folder = "{0}/{1}".format(local, "/".join(remote.name.split('/')[:-1]))
        if not os.path.isdir(folder):
            os.makedirs(folder)

        downloaded = remote.get_contents_to_filename("{0}/{1}".format(local, remote.name.split('/')[-1:]))
        return downloaded

    def make_folder(self, bucket, name):
        if not name.endswith("/"):
            name += "/"
        # if the folder already exists, just return it.
        folder = bucket.get_key(name)
        if folder:
            return folder

        progress = ""
        for part in name.split("/"):
            progress += part + "/"
            if not bucket.get_key(progress):
                key = bucket.new_key(progress)
                key.set_contents_from_string('')
                key.set_canned_acl('private')

        return self.bucket.get_key(name)

    def multipart_upload(self, bucket, filesize, file_handle, path):
        folder = self.make_folder(bucket, path)
        s3_filename = os.path.basename(file_handle.name)
        print("Using multi-part upload for {0}...".format(file_handle.name))
        mp = bucket.initiate_multipart_upload(folder.name + s3_filename)
        chunk_count = int(math.ceil(filesize / float(self.part_size)))

        for i in range(chunk_count):
            offset = self.part_size * i
            bytes = min(self.part_size, filesize - offset)
            with FileChunkIO(file_handle.name, 'r', offset=offset, bytes=bytes) as fp:
                mp.upload_part_from_file(fp, part_num=i + 1)

        print("Upload complete.")
        mp.complete_upload()

    def upload(self, bucket, file_handle, path):
        folder = self.make_folder(bucket, path)
        s3_filename = os.path.basename(file_handle.name)
        s3_file = bucket.new_key(folder.name + s3_filename)
        return s3_file.set_contents_from_file(file_handle)

    @abstractmethod
    def execute(self):
        pass


class MySQLBase(S3Base):
    def __init__(self, args):
        super(MySQLBase, self).__init__(args)
        self.username = self.args.username
        self.db_name = self.args.name

        self.version = self.args.version
        self.cleanup = self.args.cleanup

    @abstractmethod
    def execute(self):
        pass

    @property
    def password(self):
        if self.args.password:
            password = self.args.password
        else:
            password = getpass.getpass()

        return password

    def backup(self):
        gzip = "{1}/{0}/{1}.sql.gz".format(self.version, self.db_name)
        cmd = "mysqldump -u {0} -p'{1}' {2} | gzip -9 > {3}".format(self.username, self.password, self.db_name, gzip)
        os.system(cmd)

        return gzip

    def restore(self):
        cmd = "gunzip {1}.sql.gz".format(self.db_name)
        os.system(cmd)

        extracted = "{1}/{0}/{1}.sql".format(self.version, self.db_name)
        cmd = "mysql -u {0} -p'{1}' {2} <  {3}.sql".format(self.username, self.password, self.db_name, extracted)
        os.system(cmd)

        return extracted