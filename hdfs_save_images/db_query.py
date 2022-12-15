"""Database queries module."""
from typing import List

from construction_monitoring.db import Session, session_scope
from construction_monitoring.models import Snapshot


def get_is_not_saved_hdfs_snapshots() -> List[Snapshot]:
    """Get all processed snapshots from db."""
    with Session() as session:
        snapshots = session.query(Snapshot).filter(
            Snapshot.is_saved,
            Snapshot.is_processed,
            Snapshot.is_saved_hdfs.isnot(True),
            # Snapshot.snapshot_id.between(18418, 18608),  # Use for test
            ).all()
    return snapshots


def update_snapshots_data(snapshots: List[Snapshot]) -> None:
    """Update snapshots data in db."""
    with session_scope() as session:
        session.add_all(snapshots)
