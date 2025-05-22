import sys
from pathlib import Path
import psutil
import multiprocessing
from multiprocessing import Process, Queue

from src.utils.write_sbatch import SDRFWriter,SBatchScriptWriter
from src.utils.copy_and_sbatch import HPCJobManager

class RunQuantms():     
    def __init__(self, stop_queue, progress_queue,
        conditions_file, local_folder, read_files, output_folder, hpc_dir, hpc_user, hpc_host, private_key_path, job_id=None):

        self.conditions_file = conditions_file
        self.local_folder = local_folder
        self.read_files = read_files
        self.output_folder = output_folder
        self.hpc_dir = hpc_dir
        self.hpc_user = hpc_user
        self.hpc_host = hpc_host
        self.private_key_path = private_key_path

        if job_id:
            self.job_id = job_id
        else:
            self.job_id = "Quantms_run"

        if stop_queue:
            self.stop_queue = stop_queue
        else:
            self.stop_queue = Queue()
        if progress_queue:
            self.progress_queue = progress_queue
        else:
            self.progress_queue = Queue()
        
        self.sentinel_file = None
        self.log_file = None

        self.error_flag = False
        self.stop_requested = False
    
    def make_output_files(self):
        self.sentinel_file = Path(self.local_folder) / "stop.sentinel"
        self.log_file = Path(self.local_folder) / "runtime_log.log"

    def terminate_external_processes(self, subprocess_handle):
        if subprocess_handle:
            print(f"PID of subprocess to be terminated: {subprocess_handle.pid}")
            self.progress_queue.put(f"UPDATE: Terminating external process with id {subprocess_handle.pid} and all child processes.")
            self.terminate_process_tree(subprocess_handle.pid)

    def terminate_process_tree(self, pid):
        try:
            parent = psutil.Process(pid)
            print(f"Attempting to terminate parent process with PID: {parent.pid}")
            for child in parent.children(recursive=True):
                print(f"Forcefully killing child process with PID: {child.pid}")
                child.kill()
            print(f"Forcefully killing parent process with PID: {parent.pid}")
            parent.kill()
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} no longer exists.")

    def check_for_stop_signal(self):
        if not self.stop_queue.empty():
            message = self.stop_queue.get()
            if message.startswith("STOP"):
                self.stop_requested = True
                self.progress_queue.put("UPDATE: Stop requested")
        # TODO: Must get job name to allow cancel to HPC
        if self.error_flag:
            self.progress_queue.put("ERROR: Quantms cancelled due to unrecoverable errors.")
            sys.exit()

    def run_quantms(self):
        try:
            print("1")
            self.progress_queue.put("STARTING PROCESS: Converting conditions file to sdrf format.")
            sdrf_file = Path(Path(self.output_folder) / f'{str(Path(self.conditions_file).stem)}.sdrf').as_posix()
            sbatch_file = f"{Path(self.output_folder) / f'{self.job_id}'}_sbatch.sh"
            sdfr_writer = SDRFWriter(
                conditions_file=self.conditions_file,
                op_file = sdrf_file,
                read_dir = self.hpc_dir
            )
            sdfr_writer.write_sdrf()
            self.progress_queue.put("STEP COMPLETED: sdrf file written.")
            print("2")
            self.progress_queue.put("STARTING PROCESS: Writing HPC batch command.")
            script_writer = SBatchScriptWriter(
                sdrf_file=sdrf_file,
                save_path=sbatch_file,
                job_id=self.job_id,
            )
            script_writer.write_script()
            self.progress_queue.put("STEP COMPLETED: Batch command writtn.")
            print("3")
            self.progress_queue.put("STARTING PROCESS: Copying files and submitting to HPC.")
            job_manager = HPCJobManager(
                local_dir=self.local_folder,
                hpc_user=self.hpc_user,
                hpc_host=self.hpc_host,
                hpc_dir=self.hpc_dir,
                sbatch_script_local=sbatch_file,
                local_files=self.read_files,
                sdrf_file = sdrf_file,
                private_key_path=self.private_key_path,
                progress_queue=self.progress_queue
            )
            job_manager.execute()
            stdout = "PRETEND FINISH"

            with open(self.log_file, 'w') as log_handle:
                log_handle.write(str(stdout))
            
            self.progress_queue.put("STEP COMPLETED: Files copied and job submitted!")
            
        except Exception as e:
            self.progress_queue.put(
            f"ERROR: Error encountered while running Quantms: {e}"
            )
            print(f"{e}")
            self.error_flag = True
    
    def execute_quantms_workflow(self):
        self.make_output_files()
        self.check_for_stop_signal()
        self.run_quantms()
        self.check_for_stop_signal()
