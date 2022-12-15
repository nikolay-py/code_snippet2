"""Snapshot save module in hdfs."""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from construction_monitoring.hdfs_save_images.db_query import (
    get_is_not_saved_hdfs_snapshots,
)
from construction_monitoring.hdfs_save_images.save_images_to_hdfs import (
    transfer_snapshots,
)
from construction_monitoring.utils import users_log


def transfer_processed_snapshots_to_hdfs() -> None:
    """Process collected snapshots."""
    snapshots = get_is_not_saved_hdfs_snapshots()
    users_log(len(snapshots), 'Received snapshots')
    if not snapshots:
        users_log('No matching snapshots', 'Process  returned None')
        return None
    transfer_snapshots(snapshots)


if __name__ == '__main__':
    transfer_processed_snapshots_to_hdfs()
