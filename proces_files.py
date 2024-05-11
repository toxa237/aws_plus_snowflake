import re
from pyspark.sql.functions import udf, explode, lit
from pyspark.sql.types import BooleanType, StringType
from datetime import datetime


def file_proces(spark, s3, arr):
    while True:
        file_name = arr.get_next()
        try:
            file_type = re.findall(r'(videos|users|events)', file_name)[0]
            match file_type:
                case 'videos':
                    videos_file_proses(spark, file_name)
                case 'users':
                    users_file_proses(spark, file_name)
                case 'events':
                    events_file_proses(spark, s3, file_name)
                case _:
                    raise Exception('unknown type')
            s3.delete_object(Bucket='projectdt', Key=file_name)
        except Exception as e:
            print('err', file_name, e)

        arr.pop(file_name)


def videos_file_proses(spark, file_name):
    df = spark.read.csv(f's3a://projectdt/{file_name}', header=True)
    df = df.withColumn('valid', validation_video(df['name'], df['url'], df['creation_timestamp'],
                                                 df['creator_id'], df['private']))
    valid = df.filter(df['valid'] == True)
    invalid = df.filter(df['valid'] == False)
    save_csv(valid, invalid, 'videos', file_name.split('/')[-1])


def users_file_proses(spark, file_name):
    df = spark.read.csv(f's3a://projectdt/{file_name}', header=True)
    df = df.withColumn('valid', validation_users(df['fname'], df['lname'], df['email'],
                                                 df['country'], df['updated']))
    valid = df.filter(df['valid'] == True)
    invalid = df.filter(df['valid'] == False)
    save_csv(valid, invalid, 'users', file_name.split('/')[-1])


def events_file_proses(spark, s3, file_name):
    file = s3.get_object(Bucket='projectdt', Key=file_name)['Body'].read().decode()
    df = spark.read.json(spark.sparkContext.parallelize(
        re.sub(r'}{', '}\n{', re.sub(r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)',
                                     '',
                                     file)).split('\n')
    ))
    df2 = df.select(explode('events').alias('objects')).select('objects.*')
    df = df.drop('events')
    for i in set(df.columns) - set(df2.columns):
        df2 = df2.withColumn(i, lit(None).cast(df.schema[i].dataType))
    df = df.union(df2.select(df.columns)).dropna(how='all')
    df = df.withColumn('timestamp', to_normal_time(df['timestamp']))
    df = df.withColumn('valid', validation_events(df['comment'], df['event'], df['tags'],
                                                  df['timestamp'], df['user_id'], df['video_id']))
    valid = df.filter(df['valid'] == True)
    invalid = df.filter(df['valid'] == False)
    save_json(valid, invalid, 'events', file_name.split('/')[-1])


@udf(BooleanType())
def validation_video(name, url, creation_timestamp, creator_id, private):
    if not isinstance(name, str):
        return False
    if not re.match(r'^https?://[^\s<>"]+|www\.[^\s<>"]+$', url):
        return False
    if not re.match(r'^\d+$', creation_timestamp):
        return False
    if not re.match(r'^\d+$', creator_id):
        return False
    if not re.match(r'^[01]$', private):
        return False
    return True


@udf(BooleanType())
def validation_users(fname, lname, email, country, updated):
    if not isinstance(fname, str):
        return False
    if not isinstance(lname, str):
        return False
    if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email):
        return False
    if not re.match(r'^[A-Z]{2}$', country):
        return False
    try:
        datetime.strptime(updated, '%d.%m.%YT%H:%M:%S')
    except:
        return False
    return True


@udf(BooleanType())
def validation_events(comment, event, tags, timestamp, user_id, video_id):
    if None in [timestamp, user_id, video_id, event]:
        return False
    if event == 'commented' and comment is None:
        return False
    if (event == 'add_tags' or event == 'remove_tags') and tags is None:
        return False
    return True


@udf(StringType())
def to_normal_time(x):
    return str(datetime.fromtimestamp(int(x)))


def save_csv(valid, invalid, folder, file_name):
    if valid.count():
        valid.drop('valid').write.csv(f's3a://projectdt/{folder}_right/{file_name}', mode="overwrite")
    if invalid.count():
        invalid.drop('valid').write.csv(f's3a://projectdt/{folder}_wrong/{file_name}', mode="overwrite")


def save_json(valid, invalid, folder, file_name):
    if valid.count():
        valid.drop('valid').write.json(f's3a://projectdt/{folder}_right/{file_name}', mode="overwrite")
    if invalid.count():
        invalid.drop('valid').write.json(f's3a://projectdt/{folder}_wrong/{file_name}', mode="overwrite")
