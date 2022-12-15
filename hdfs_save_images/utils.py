"""Utils using current business logic."""
import os
import shutil
from datetime import datetime
from threading import BoundedSemaphore, Thread
from typing import List

from construction_monitoring.init_config import Config
from construction_monitoring.models import Snapshot
from construction_monitoring.utils import users_log
from construction_monitoring.utils_hdfs import copy_to_hdfs

THREADS_POOL = BoundedSemaphore(value=Config.MAX_THREAD_COUNT)


def copy_snapshot_to_tepm_local(snapshot: Snapshot) -> None:
    """Copy images to tepm local common dir."""
    with THREADS_POOL:
        try:
            shutil.copy(snapshot.local_path, snapshot.local_path_temp)
            snapshot.is_saved_hdfs = True
            # if not delete - path be used again
            delattr(snapshot, 'local_path')
        except Exception as e:
            msg = f'Can not copy {snapshot.local_path} to \
                    {snapshot.local_path_temp}'
            users_log(e, msg)


def copy_img_to_tepm_local_with_multi(
        snapshots: List[Snapshot]) -> List[Snapshot]:
    """Start multiprocessing and copy images."""
    threads = [
        Thread(
            target=copy_snapshot_to_tepm_local,
            args=(snapshot,),
            name=f'thr-{snapshot.snapshot_id}',
        )
        for snapshot in snapshots if getattr(snapshot, 'local_path', None)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    return snapshots


def get_path_to_img(snapshots: List[Snapshot], topic: str,
                    source_path: str, attribut_fname: str,
                    tmp_local_path: str) -> List[Snapshot]:
    """Get local path to img and create new filename."""
    for snapshot in snapshots:
        img_dir = snapshot.image_path.split('/')[0]
        new_filename = str(img_dir + '_' + topic + '.jpg')
        local_path = os.path.join(source_path, img_dir, topic + '.jpg')
        local_path_temp = os.path.join(
            tmp_local_path, new_filename)

        if os.path.isfile(local_path):
            setattr(snapshot, 'local_path', local_path)  # noqa
            setattr(snapshot, 'local_path_temp', local_path_temp)  # noqa
            setattr(snapshot, attribut_fname, new_filename)

    return snapshots


def sort_and_copy_to_temp_hdfs(snapshots: List[Snapshot],
                               topics: List[str],
                               source_path: str,
                               tmp_hdfs_path: str) -> List[Snapshot]:
    """Sort by topic and copy images to tepm local topic directory \
       Copy images from temp local to temp hdfs."""
    for topic in topics:
        # Get paths
        temp_dir = datetime.today().strftime('temp_%Y%m%d')
        tmp_local_path = os.path.join(source_path, temp_dir, topic)
        os.makedirs(tmp_local_path, exist_ok=True)
        attribut_fname = f'hdfs_{topic}_name'

        snapshots = get_path_to_img(
            snapshots, topic, source_path, attribut_fname, tmp_local_path,
        )
        users_log(f'Copy all {topic}.jpg to {tmp_local_path}')
        snapshots = copy_img_to_tepm_local_with_multi(snapshots)
        copy_to_hdfs(tmp_local_path, tmp_hdfs_path)
        try:
            shutil.rmtree(tmp_local_path)
            users_log(f'directory {tmp_local_path} was deleted')
        except Exception as e:
            users_log(f'Can not delete {tmp_local_path}\n{e}')

    return snapshots
