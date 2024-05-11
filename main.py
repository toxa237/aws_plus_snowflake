import os
import threading
import time
import boto3
from pyspark import SparkConf
from pyspark.sql import SparkSession
from proces_files import file_proces
from dotenv import dotenv_values


class ArrayQueue:
    def __init__(self, arr=None):
        if arr is None:
            arr = []
        self.arr = set(arr)
        self.in_proses = set()
        self.cv = threading.Condition()

    def update(self, new_files):
        with self.cv:
            self.arr.update(new_files)
            self.arr = self.arr - self.in_proses
            self.arr.remove('input_files/')
            self.cv.notify()

    def get_next(self):
        with self.cv:
            while not self.arr:
                self.cv.wait()
            next_val = self.arr.pop()
            self.in_proses.update({next_val})
            return next_val

    def pop(self, val):
        self.in_proses.remove(val)


def update_files_list(s3, arr):
    while True:
        new_files = s3.list_objects(Bucket=bucket, Prefix='input_files/').get('Contents')
        if new_files:
            arr.update([o.get('Key') for o in new_files])
        time.sleep(10)


if __name__ == '__main__':
    bucket = 'projectdt'
    SECRET_KEYS = dotenv_values(dotenv_path='.env')
    conf = SparkConf()
    conf.set('spark.hadoop.fs.s3a.access.key', SECRET_KEYS['ACCESS_KEY'])
    conf.set('spark.hadoop.fs.s3a.secret.key', SECRET_KEYS['SECRET_KEY'])
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    S3 = boto3.client('s3')
    list_items = ArrayQueue()
    tr1 = threading.Thread(target=update_files_list, args=(S3, list_items,))
    tr2 = threading.Thread(target=file_proces, args=(spark, S3, list_items,))

    try:
        tr1.start()
        tr2.start()
    except KeyboardInterrupt:
        pass

    for i in os.listdir('DataProject'):
        if 'events' not in i:
            continue
        with open(f'DataProject/{i}', 'rb') as f:
            S3.upload_fileobj(f, "projectdt", f"input_files/{i}")

