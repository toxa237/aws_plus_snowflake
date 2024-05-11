USE DATABASE AWS_CLOUD;


CREATE OR REPLACE STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::590183885018:role/snowflake_r'
    STORAGE_ALLOWED_LOCATIONS = ('s3://projectdt/events_right/', 's3://projectdt/users_right/', 's3://projectdt/videos_right/')


CREATE OR REPLACE FILE FORMAT s3_videos_CSV
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 0;


CREATE OR REPLACE FILE FORMAT S3_USERS_CSV
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    timestamp_format = 'DD.MM.YYYYTHH24:MI:SS'
    SKIP_HEADER = 0;


CREATE OR REPLACE FILE FORMAT s3_events_sjson
    TYPE = 'JSON'
    timestamp_format = 'YYYY-MM-DD HH24:MI:SS';


CREATE OR REPLACE STAGE s3_videos_csv_stage
    FILE_FORMAT = s3_videos_CSV
    URL = 's3://projectdt/videos_right/'
    storage_integration = s3_int;


CREATE OR REPLACE STAGE s3_users_csv_stage
    FILE_FORMAT = S3_USERS_CSV
    URL = 's3://projectdt/users_right/'
    storage_integration = s3_int;


CREATE OR REPLACE STAGE s3_events_json_stage
    FILE_FORMAT = s3_events_sjson
    URL = 's3://projectdt/events_right/'
    storage_integration = s3_int;


CREATE OR REPLACE TABLE videos(
    id INTEGER,
    name VARCHAR,
    url VARCHAR,
    creation_timestamp VARCHAR,
    creator_id INTEGER,
    private INTEGER
);


CREATE OR REPLACE TABLE users_data(
    id INTEGER,
    fname VARCHAR,
    lname VARCHAR,
    email VARCHAR,
    country VARCHAR,
    subscription INTEGER,
    categories VARCHAR,
    updated datetime
);


CREATE OR REPLACE TABLE events(
    user_id VARCHAR,
    video_id VARCHAR,
    event VARCHAR,
    timestamp datetime,
    tags ARRAY,
    comment VARCHAR
);