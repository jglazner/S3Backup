__author__ = 'jglazner'

import boto
import logging
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
        super(S3Base).__init__(args)
        self.conn = boto.connect_s3()

    def get_or_create_bucket(self, bucket_name):
        bucket = None
        try:
            bucket = self.conn.get_bucket(bucket_name)
        except S3ResponseError:
            bucket = self.conn.create_bucket(bucket_name)
            bucket.set_canned_acl('private')

        return bucket

    @abstractmethod
    def execute(self):
        pass