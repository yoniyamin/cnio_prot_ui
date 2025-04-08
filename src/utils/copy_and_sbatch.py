import logging
import os
from pathlib import Path
import paramiko

class HPCJobManager:
    def __init__(self, local_dir, hpc_user, hpc_host, hpc_dir, sbatch_script_local, sdrf_file, local_files, private_key_path, progress_queue=None, stop_queue=None):
        self.local_dir = Path(local_dir)
        self.hpc_user = hpc_user
        self.hpc_host = hpc_host
        self.hpc_dir = hpc_dir
        self.sbatch_script_local = Path(sbatch_script_local)
        self.sdrf_file = Path(sdrf_file)
        self.local_files = local_files
        self.private_key_path = Path(private_key_path)
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(self.__class__.__name__)

        self.ssh_client = None
        self.batch_id = None

        self.progress_queue = progress_queue
        self.stop_queue = stop_queue


    def connect_to_hpc(self):
        """Establish SSH connection to HPC."""
        self.logger.info("Connecting to HPC...")
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(
            hostname=self.hpc_host,
            username=self.hpc_user,
            key_filename=str(self.private_key_path)
        )

    def disconnect_from_hpc(self):
        """Close SSH connection."""
        if self.ssh_client:
            self.logger.info("Disconnecting from HPC...")
            self.ssh_client.close()
    

    def copy_sdrf_to_hpc(self, max_retries=3):
        self.logger.info("Copying sdrf script via SFTP...")
        retries = 0
        while retries < max_retries:
            try:
                sftp = self.ssh_client.open_sftp()
                remote_path = f"{self.hpc_dir}/{self.sdrf_file.name}"
                sftp.put(str(self.sdrf_file), remote_path)
                sftp.close()
                self.logger.info(f"Successfully copied sdrf script to {remote_path}")
                return True
            except paramiko.SSHException as e:
                self.logger.error(f"SSHException during SFTP: {e}")
                retries += 1
                self.logger.info(f"Retrying... Attempt {retries}/{max_retries}")
                self.connect_to_hpc()  # Reconnect before retrying
            except Exception as e:
                self.logger.error(f"Error copying sdrf script: {e}")
                return False
        return False

    def copy_files_to_hpc(self, recursive=True):
        """Copy files to HPC using SFTP."""
        self.logger.info("Starting file transfer via SFTP...")
        sftp = self.ssh_client.open_sftp()
        try:
            if recursive:
                # Traverse through the local directory and its subdirectories
                processed_files = set()
                for root, _, files in os.walk(self.local_dir):
                    for file in files:
                        if (file in processed_files or file not in self.local_files):
                            continue  # Skip if the file has already been processed
                        local_path = Path(root) / file
                        remote_path = f"{self.hpc_dir}/{file}"
                        if self.check_existing_files(sftp, remote_path)==False:
                            if local_path.exists():
                                self.logger.info(f"Copying {local_path} to {remote_path}...")
                                try:
                                    sftp.put(str(local_path), remote_path)
                                    processed_files.add(file)                                   
                                except Exception as e:
                                    self.logger.error(f"Error copying {local_path}: {e}")
                                    sftp.close()
                                    return False
                    
            else:
                for file in self.local_files:
                    local_path = Path(self.local_dir) / file
                    remote_path = f"{self.hpc_dir}/{file}"
                    if self.check_existing_files(sftp, remote_path) == False:
                        self.logger.info(f"Copying {file} to {remote_path}...")
                        try:
                            sftp.put(str(local_path), remote_path)
                        except Exception as e:
                            self.logger.error(f"Error copying {file}: {e}")
                            sftp.close()
                            return False
        finally:
            sftp.close()
        return True
    
    def check_existing_files(self, sftp, remote_path):
        try:
            sftp.stat(remote_path)
            self.logger.info(f"File already exists: {remote_path}, skipping.")
            return True
        except:
            self.logger.info(f"File does not exist, proceeding to copy: {remote_path}")
            return False

    def copy_sbatch_script_to_hpc(self):
        """Copy sbatch script to HPC using SFTP."""
        self.logger.info("Copying sbatch script via SFTP...")
        sftp = self.ssh_client.open_sftp()
        try:
            remote_path = f"{self.hpc_dir}/{self.sbatch_script_local.name}"
            sftp.put(str(self.sbatch_script_local), remote_path)
        except Exception as e:
            self.logger.error(f"Error copying sbatch script: {e}")
            sftp.close()
            return False
        sftp.close()
        return True

    def submit_sbatch(self):
        """Submit sbatch job via SSH."""
        sbatch_command = f"cd {self.hpc_dir} && sbatch {self.hpc_dir}/{self.sbatch_script_local.name}"
        self.logger.info(f"Submitting sbatch command: {sbatch_command}")
        try:
            stdin, stdout, stderr = self.ssh_client.exec_command(sbatch_command)
            stdout_text = stdout.read().decode()
            stderr_text = stderr.read().decode()
            if stderr_text:
                self.logger.error(f"Error submitting sbatch command: {stderr_text}")
                return False
            self.logger.info(f"Job submitted successfully: {stdout_text}")
            if self.progress_queue:
                self.progress_queue.put(f"{stdout_text}")
            return True
        except Exception as e:
            self.logger.error(f"Error executing sbatch command: {e}")
            return False

    def execute(self):
        """Execute the workflow."""
        self.connect_to_hpc()
        try:
            if self.copy_files_to_hpc():
                self.logger.info("File transfer completed. Copying sbatch script...")
                if (self.copy_sbatch_script_to_hpc() and self.copy_sdrf_to_hpc()):
                    self.logger.info("Sbatch script and sdrf file copied. Submitting sbatch job...")
                    if self.submit_sbatch():
                        self.logger.info("Job submitted successfully.")
                    else:
                        self.logger.error("Failed to submit sbatch job.")
                else:
                    self.logger.error("Failed to copy sbatch script.")
            else:
                self.logger.error("File transfer failed.")
        finally:
            self.disconnect_from_hpc()

if __name__ == "__main__":
    # Example usage
    job_manager = HPCJobManager(
        local_dir=r"C:\Users\lwoods\Documents\LW_Projects_folder\HPC",
        hpc_user="lwoods",
        hpc_host="cluster1.cnio.es",
        hpc_dir="/storage/scratch01/groups/pcu/pancaid_2025/quantms",
        sbatch_script_local=r"C:\Users\lwoods\Documents\LW_Projects_folder\HPC\test_dia_sbatch2.sh",
        local_files=[],
        sdrf_file = r"C:\Users\lwoods\Documents\LW_Projects_folder\HPC\demo_conditions.sdrf",
        private_key_path=r"C:\Users\lwoods\Documents\LW_Projects_folder\HPC\HPC_id_openssh",
    )
    job_manager.execute()