USE DATABASE AWS_CLOUD;

COPY INTO videos
  FROM @s3_videos_csv_stage
  ON_ERROR = 'skip_file';


COPY INTO users_data
    FROM @s3_users_csv_stage
    ON_ERROR = 'skip_file';


COPY INTO events FROM (
    SELECT t.$1:user_id, t.$1:video_id,
    t.$1:event, t.$1:timestamp, t.$1:tags, t.$1:comment
    FROM @s3_events_json_stage as t
);