import os
from re import I
from airflow.models.baseoperator import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from scp import SCPClient
from airflow.exceptions import AirflowException


class SparkOperator(BaseOperator):
    """
    Airflow operator which runs spark jobs over SSH.

    Params:
    - ssh_conn_id (string): airflow ssh connection id to spark cluster
    - script_file (string): name of script file to run on the spark cluster
    """

    def __init__(self, ssh_conn_id="ssh_emr", script_file=None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.script_file = script_file
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context):
        ssh_hook = SSHHook(self.ssh_conn_id)
        ssh_conn = ssh_hook.get_conn()
        scp = SCPClient(ssh_conn.get_transport())

        self.log.info(f"Copying spark files")
        ssh_conn.exec_command(f"rm -rf {self.task_id}")
        scp.put(os.path.join(os.getcwd(), f"spark"),
                recursive=True, remote_path=f"~/{self.task_id}")

        self.log.info(f"Running spark job {self.task_id}/{self.script_file}")
        _, stdout, stderr = ssh_conn.exec_command(
            f"spark-submit --master yarn {self.task_id}/{self.script_file}")
        stdout = stdout.readlines()
        stdout_str = " ".join(stdout)
        stderr = stderr.readlines()

        if not stderr:
            raise AirflowException(
                "Spark job failed '{}'".format("\n".join(stderr)))
        if "Exception" in stdout_str or "Error" in stdout_str:
            raise AirflowException(
                "Spark job failed '{}'".format(stdout_str))

        if stdout:
            self.log.info(f"Spark job returned result:")
            self.log.info(stdout_str)
