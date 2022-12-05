import logging
import subprocess

logger = logging.getLogger()


def run_cmdb(cmdb: str, check=True) -> str:
    """General command execution.

    Args:
        cmdb (str): command to execute
        check (bool, optional): catch erorrs during cmd execution.
            Defaults to True.

    Returns:
        str: stdout decoded message
    """
    try:
        cmdb_response = subprocess.run(
            cmdb.split(),
            capture_output=True,
            check=check
        )
    except subprocess.CalledProcessError as e:
        logger.warning(
            f"Error while exec cmdb {cmdb}. Return code: {e.returncode}. "
            f"Stdout: {e.stdout.decode('utf-8').strip()}. "
            f"Stderr: {e.stderr.decode('utf-8').strip()}"
            )
        raise
    else:
        return cmdb_response.stdout.decode('utf-8').strip()
