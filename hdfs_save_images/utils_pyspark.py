"""Utils using pyspark."""
import os
from typing import NamedTuple

from construction_monitoring.init_config import Config

from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f


conf = SparkConf().setAppName('csm_save_snapshots_to_hdfs')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
spark = SparkSession(sc)


class Topic(NamedTuple):
    """List snapshot's topic and table name for it."""

    snapshot: str = 'snapshot'
    transform: str = 'transform'
    building: str = 'building'
    result: str = 'result'

    bi_snapshot: str = 'snapshot_binary'
    bi_transform: str = 'transform_binary'
    bi_building: str = 'building_binary'
    bi_result: str = 'result_binary'


def read_and_write_snapshot(tmp_hdfs_path: str,
                            target_hdfs_path: str) -> None:
    """Read, join, and write table snapshots with binary images."""
    print(f'Read and write snapshots \
            from {tmp_hdfs_path} \
            to {target_hdfs_path}')

    topic = Topic()
    snapshots_table_df = get_table_snapshots()

    snapshot_df = \
        get_df_binary_topic(tmp_hdfs_path, topic.snapshot, topic.bi_snapshot)
    transform_df = \
        get_df_binary_topic(tmp_hdfs_path, topic.transform, topic.bi_transform)
    building_df = \
        get_df_binary_topic(tmp_hdfs_path, topic.building, topic.bi_building)
    result_df = \
        get_df_binary_topic(tmp_hdfs_path, topic.result, topic.bi_result)

    binary_df = snapshot_df \
        .join(transform_df, snapshot_df.img_id == transform_df.img_id) \
        .join(building_df, snapshot_df.img_id == building_df.img_id) \
        .join(result_df, snapshot_df.img_id == result_df.img_id) \
        .select(
            snapshot_df['*'],
            transform_df[topic.bi_transform],
            building_df[topic.bi_building],
            result_df[topic.bi_result],
        )

    binary_df2 = binary_df \
        .join(snapshots_table_df,
              binary_df.img_id == snapshots_table_df.snapshot_id) \
        .select(
            snapshots_table_df['*'],
            binary_df[topic.bi_snapshot],
            binary_df[topic.bi_transform],
            binary_df[topic.bi_building],
            binary_df[topic.bi_result],
        )

    binary_df3 = binary_df2 \
        .withColumn(
            'datekey',
            f.date_format(binary_df2.created_at, 'yyyyMMdd'),
        )

    binary_df3 \
        .repartition(1) \
        .write.mode('append') \
        .partitionBy('datekey') \
        .parquet(target_hdfs_path)

    spark.stop()


def get_df_binary_topic(tmp_hdfs_path: str,
                        topic: str, table_name: str) -> DataFrame:
    """Get df with an image id and its bytes."""
    topic_tmp_hdfs_path = os.path.join(tmp_hdfs_path, topic)

    binary_df = spark.read \
        .format('binaryFile') \
        .option('recursiveFileLookup', 'true') \
        .load(topic_tmp_hdfs_path)

    binary_df2 = binary_df \
        .select(binary_df.content, binary_df.path) \
        .withColumnRenamed('content', table_name) \
        .withColumn(
            'img_name', f.element_at(f.split(binary_df.path, '/'), -1)) \
        .select('img_name', table_name)

    binary_df3 = binary_df2 \
        .withColumn(
            'img_id', f.element_at(f.split(binary_df2.img_name, '_'), 1)) \
        .select('img_id', table_name)

    return binary_df3


def get_table_snapshots() -> DataFrame:
    """Get df from snapshots table db."""
    db_conf = {
        'url': Config.SPARK_DATABASE_URL,
        'dbtable': 'public.snapshots',
        'user': Config.DB_USER,
        'password': Config.DB_PASSWORD,
        'driver': 'org.postgresql.Driver',
    }
    snapshots_df = spark.read.format('jdbc').options(**db_conf).load()

    return snapshots_df
