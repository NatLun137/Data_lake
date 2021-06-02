from pyspark.sql import SparkSession
import os
import configparser
import pyspark.sql.functions as F


def create_spark_session():
    """
    Creates a spark session.
    Returns:
        spark: a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def transform_events(df_events_raw):
    """
    The function transforms raw spark dataframe into three dataframes which are the prototypes of the tables
    with a star schema optimized for queries on song play analysis.

    Args:
        df_events_raw (dataframe): The events-dataframe.
    Returns:
        time_df: time-dataframe
        users_df: users-dataframe
        songplays_proto_df: songplays-dataframe with data related to events
    """
    df_events = df_events_raw.filter(F.col('page') == 'NextSong')  # df_events.count() = 6820
    ts_transform = F.to_timestamp(F.from_unixtime(F.col("ts") / 1000, 'yyyy-MM-dd HH:mm:ss'))

    dt = df_events.withColumn("start_time", ts_transform) \
        .withColumn("hour", F.hour(ts_transform)) \
        .withColumn("day", F.dayofmonth(ts_transform)) \
        .withColumn("week", F.weekofyear(ts_transform)) \
        .withColumn("month", F.month(ts_transform)) \
        .withColumn("year", F.year(ts_transform)) \
        .withColumn("weekday", F.date_format(ts_transform, "EEEE"))

    users_cols = ["userId", "firstName", "lastName", "gender", "level", "sessionId"]
    users_cols_new_names = ["user_id", "first_name", "last_name", "gender", "level", "session_id"]
    name_mapping = dict(zip(users_cols, users_cols_new_names))

    time_cols = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df_ = dt.select(*time_cols)
    time_df = time_df_.distinct()  # 6813

    users_df_ = dt.select(*users_cols)
    users_df_ = users_df_.select([F.col(c).alias(name_mapping.get(c, c)) for c in users_df_.columns])
    users_df = users_df_.distinct()  # 784

    songplays_proto_cols = ["song", "artist", "start_time", "userId", "level", "sessionId", "location", "userAgent"]
    songplays_proto_cols_new = ["song", "artist", "start_time", "user_id", "level", "session_id", "location",
                                "user_agent"]
    name_mapping2 = dict(zip(songplays_proto_cols, songplays_proto_cols_new))

    songplays_proto_df_ = dt.select(*songplays_proto_cols)
    songplays_proto_df = songplays_proto_df_.select(
        [F.col(c).alias(name_mapping2.get(c, c)) for c in songplays_proto_df_.columns])

    return time_df, users_df, songplays_proto_df


def transform_songs(df_songs_raw):
    """
        The function transforms raw spark dataframe into three dataframes which are the prototypes of the tables
        with a star schema optimized for queries on song play analysis.

        Args:
            df_songs_raw (dataframe): The songs-dataframe.
        Returns:
            songs_df: songs-dataframe
            artists_df: artists-dataframe
            songplays_proto2_df: songplays-dataframe with data related to songs
    """
    songs_cols = ["song_id", "title", "artist_id", "year", "duration"]
    songs_df_ = df_songs_raw.select(*songs_cols)
    songs_df = songs_df_.distinct()  # in the subset 604

    artists_cols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_cols_new = ["artist_id", "name", "location", "latitude", "longitude"]

    name_mapping = dict(zip(artists_cols, artists_cols_new))
    artists_df_ = df_songs_raw.select(*artists_cols)
    artists_df_ = artists_df_.select([F.col(c).alias(name_mapping.get(c, c)) for c in artists_df_.columns])
    artists_df = artists_df_.distinct()  # in the subset 591

    songplays_proto2_cols = ["artist_name", "title", "artist_id", "song_id"]

    songplays_proto2_df_ = df_songs_raw.select(*songplays_proto2_cols)
    songplays_proto2_df = songplays_proto2_df_.distinct()  # in the subset 604
    return songs_df, artists_df, songplays_proto2_df


def main():
    """
    The ETL pipeline to create a star schema optimized for queries on song play analysis.
    """
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    # create a spark session
    spark = create_spark_session()

    # reading in raw data
    df_events_raw = spark.read.json("s3a://udacity-dend/log_data/2018/11/*.json")
    df_songs_raw = spark.read.json("s3a://udacity-dend/song_data/A/A/*/*.json")  # 604 in the subset

    # transforming data
    time_df, users_df, songplays_proto_df = transform_events(df_events_raw)
    songs_df, artists_df, songplays_proto2_df = transform_songs(df_songs_raw)
    joined_df = songplays_proto_df.join(songplays_proto2_df).where(
        songplays_proto_df.song.eqNullSafe(songplays_proto2_df.title)) \
        .where(songplays_proto_df.artist.eqNullSafe(songplays_proto2_df.artist_name))  # in the subset 16

    joined_dfp = joined_df.withColumn("songplay_id", F.monotonically_increasing_id())

    songplays_cols = ["songplay_id", "start_time", "user_id", "level", "song_id", "artist_id",
                      "session_id", "location", "user_agent"]
    songplays_df = joined_dfp.select(*songplays_cols)

    songplays_df = songplays_df.withColumn("year", F.year("start_time")) \
        .withColumn("month", F.month("start_time"))

    # writing transformed data to S3 bucket.
    songplays_df.write.partitionBy("year", "month").mode("overwrite").parquet(
        "s3a://course-datalake-prj/analytics/songplays/songplays.parquet")

    songs_df.write.partitionBy("year", "artist_id").mode("overwrite").parquet(
        "s3a://course-datalake-prj/analytics/songs/songs.parquet")

    time_df.write.partitionBy("year", "month").mode("overwrite").parquet(
        "s3a://course-datalake-prj/analytics/time/time.parquet")

    users_df.write.mode("overwrite").parquet(
        "s3a://course-datalake-prj/analytics/users/users.parquet")

    artists_df.write.mode("overwrite").parquet(
        "s3a://course-datalake-prj/analytics/artists/artists.parquet")


if __name__ == "__main__":
    main()
