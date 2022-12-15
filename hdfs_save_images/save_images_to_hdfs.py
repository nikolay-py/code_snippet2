"""Snapshot save module in hdfs."""
from typing import List

from construction_monitoring.hdfs_save_images.db_query import (
    update_snapshots_data,
)
from construction_monitoring.hdfs_save_images.utils import (
    sort_and_copy_to_temp_hdfs,
)
from construction_monitoring.hdfs_save_images.utils_pyspark import (
    read_and_write_snapshot,
)
from construction_monitoring.init_config import Config
from construction_monitoring.models import Snapshot
from construction_monitoring.utils_hdfs import (
    makedir_to_hdfs,
    remove_hdfs_directory,
)


def transfer_snapshots(snapshots: List[Snapshot]) -> None:
    """
    Transfer images to hdfs.

    Get paths and attribut topic_filename.
    Create temp local topic directory.
    Sort by topic and copy snapshot to topic directory.
    Copy imges from temp local to temp hdfs
    Read snapshot as binary.
    Write to hdfs copied snapshot.
    Clear temp local directory.
    Clear temp hdfs directory.
    """
    topics = ['snapshot', 'transform', 'building', 'result']
    source_path = Config.DIR_FOR_IMAGE_SAVING
    tmp_hdfs_path = Config.TMP_HDFS_PATH
    target_hdfs_path = Config.TARGET_HDFS_PATH
    makedir_to_hdfs(tmp_hdfs_path)

    snapshots = sort_and_copy_to_temp_hdfs(
        snapshots, topics, source_path, tmp_hdfs_path,
    )
    update_snapshots_data(snapshots)
    read_and_write_snapshot(tmp_hdfs_path, target_hdfs_path)
    remove_hdfs_directory(tmp_hdfs_path)
