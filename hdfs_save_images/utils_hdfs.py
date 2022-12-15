"""Utils using subprocess hdfs command."""
from subprocess import PIPE, Popen

from construction_monitoring.utils import users_log


def remove_hdfs_directory(hdfs_path: str) -> None:
    """Remove hdfs directory."""
    msg = f'Try to remove tmp hdfs directory {hdfs_path}'
    users_log(msg)

    process = Popen(['hdfs', 'dfs', '-rm', '-R',  '-skipTrash', hdfs_path],
                    stderr=PIPE)
    std_err = process.communicate()[1].decode('utf-8')
    users_log(std_err) if process.returncode else users_log(
        f'Directory hdfs:///{hdfs_path} has been removed')


def makedir_to_hdfs(path_to_hdfs: str) -> None:
    """Create hdfs directory."""
    users_log(f'Creating directory in hdfs:///{path_to_hdfs}')

    process = Popen(
        ['hdfs', 'dfs', '-mkdir', '-p', path_to_hdfs],
        stderr=PIPE)
    std_err = process.communicate()[1].decode('utf-8')

    users_log(std_err) if process.returncode else users_log(
        f'Directory hdfs:///{path_to_hdfs} has been created')


def copy_to_hdfs(local_path: str, hdfs_path: str):
    """Copy local file to hdfs directory."""
    users_log(f'Copy from {local_path} to hdfs:///{hdfs_path}')

    process = Popen(
        ['hdfs', 'dfs', '-copyFromLocal', local_path, hdfs_path],
        stderr=PIPE)
    std_err = process.communicate()[1].decode('utf-8')

    users_log(std_err) if process.returncode else users_log(
        f'Directory {local_path} has been copy to \
          hdfs:///{hdfs_path} ')
