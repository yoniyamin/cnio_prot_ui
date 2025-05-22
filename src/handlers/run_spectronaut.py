"""Run Spectronaut from CLI"""

import subprocess
import sys
from pathlib import Path
import os
import psutil
import multiprocessing
from multiprocessing import Process, Queue
import time
import pandas as pd

class spectronaut_handler():       
    def __init__(self, stop_queue, progress_queue,
        spectronaut_exe, fasta, conditions, op_folder,
        raw_folder=None,
        config_file = None):

        self.spectronaut_exe = spectronaut_exe
        self.config_file = config_file

        self.fasta = fasta
        self.conditions = conditions
        self.op_folder = op_folder

        if stop_queue:
            self.stop_queue = stop_queue
        else:
            self.stop_queue = Queue()
        if progress_queue:
            self.progress_queue = progress_queue
        else:
            self.progress_queue = Queue()
        if raw_folder:
            self.raw_folder = raw_folder
        else:
            self.raw_folder = Path(self.op_folder) / 'raw_file_folder'
        
        self.sentinel_file = None
        self.log_file = None
        self.conditions_dict = {}

        self.error_flag = False
        self.stop_requested = False

    def make_output_folder(self):
        if Path(self.op_folder).exists():
            return
        else:
            Path.mkdir(self.op_folder, parents=True, exist_ok=True)
    
    def make_raw_data_folders(self):
        if not Path(self.raw_folder).exists():
            Path(self.raw_folder).mkdir(parents=True)
        # HTRMS?
    
    def make_output_files(self):
        self.diann_config = Path(self.op_folder) / "spectronaut_config.txt"
        self.sentinel_file = Path(self.op_folder) / "stop.sentinel"
        self.log_file = Path(self.op_folder) / "runtime_log.log" 
    
    def make_conditions_dict(self):
        print(f"{self.conditions=}")
        try:
            ext = os.path.splitext(str(Path(self.conditions)))[1]
            if ext == ".xlsx":
                conditions_df = pd.read_excel(Path(self.conditions), sheet_name="Sheet1", dtype = str)
            elif ext == ".txt" or ext == ".tsv":
                conditions_df = pd.read_csv(Path(self.conditions), sep="\t", dtype = str)
        except Exception as e:
            print(f"Error: {e}. Please use a file in excel or tab-delimited format.")
            self.error_flag = True
            return None
        
        # Add a 'Basename' column to the dataframe-takes care of if using full paths or just names
        print(conditions_df)
        conditions_df['Basename'] = conditions_df['Raw file'].apply(lambda x: os.path.basename(str(Path(x))))

        # Make column for full raw folder path
        conditions_df['Full raw path'] = conditions_df['Basename'].apply(lambda x: Path(self.raw_folder) / str(x))

        # Add column for mzML files
        # Change to htrms if required
        # self.mzml_folder = Path(self.output_folder) / "mzML_files"
        # conditions_df['Full mzML path'] = conditions_df['Basename'].apply(lambda x: (Path(self.mzml_folder) / f'{Path(x).stem}.mzML'))
        
        # Convert the dataframe to a dictionary with 'raw_file' (basename) as keys
        for _, row in conditions_df.iterrows():
            raw_file = row['Basename']
            self.conditions_dict[raw_file] = row.drop('Basename').to_dict()
        print(conditions_df)

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

    def check_for_stop_signal(self, subprocess_handle=None):
        if not self.stop_queue.empty():
            message = self.stop_queue.get()
            if message.startswith("STOP"):
                self.stop_requested = True
                self.progress_queue.put("UPDATE: Stop requested")
        elif Path(self.sentinel_file).exists():
            self.stop_requested = True
            self.progress_queue.put("PROCESS CANCELLED: Spectronaut handler cancelled.")
            sys.exit()
        if self.stop_requested and subprocess_handle:
            self.terminate_external_processes(subprocess_handle)
            self.progress_queue.put("PROCESS CANCELLED: Spectronaut handler cancelled.")
            sys.exit()
        if self.error_flag:
            self.progress_queue.put("ERROR: Spectronaut handler cancelled due to unrecoverable errors.")
            sys.exit()
    
    def write_spectronaut_command(self):
        if self.config_file:
            pass
        else:
            print("Currently not running without config file.")
            sys.exit()

    def run_spectronaut(self):
        try:
            self.progress_queue.put("STARTING PROCESS: Starting DIA-NN searches.")
            self.write_spectronaut_command()
            command_list = [str(Path(self.spectronaut_exe)), '-command', self.config_file]
            print("Spectronaut process starting...")
            print(command_list)
            r"""
            spectronaut_subprocess_handle = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            while spectronaut_subprocess_handle.poll() is None:
                self.check_for_stop_signal(spectronaut_subprocess_handle)
                time.sleep(10)
            
            stdout, stderr = spectronaut_subprocess_handle.communicate()
            print(stderr)
            """
            stdout = "PRETEND FINISH"

            with open(self.log_file, 'w') as log_handle:
                log_handle.write(str(stdout))
            
            self.progress_queue.put("STEP COMPLETED: Spectronaut search complete.")
            
        except Exception as e:
            self.progress_queue.put(
            f"ERROR: Error encountered while running Spectronaut: {e}"
            )
            print(f"{e}")
            self.error_flag = True
    
    def run_spectronaut_cli(self):
        self.make_output_folder()
        self.make_raw_data_folders()
        self.make_output_files()
        self.check_for_stop_signal()
        self.make_conditions_dict()
        self.check_for_stop_signal()
        self.run_spectronaut()
        self.check_for_stop_signal()
        self.progress_queue.put("PROCESS COMPLETED: Finished Spectronaut search handler.")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    app = spectronaut_handler(
        spectronaut_exe = r"C:\Program Files (x86)\Biognosys\Spectronaut_18\bin\Spectronaut.exe",
        fasta = r"C:\Users\lwoods\Documents\LW_Projects_folder\general\FASTA_DATABASE_Junio_2021\REFERENCE PROTEOME\20210609_Human_OPSPG_20614.fasta",
        op_folder = r"C:\Users\lwoods\Documents\spec_from_cmd_test",
        conditions = r"C:\Users\lwoods\Documents\diann_from_cmd_test\conditions2_modified.tsv",
        stop_queue = None,
        progress_queue = None,
        config_file = r"C:\Spectronaut_folders\Arguments_files\Test_arguments.txt"
        )
    app.run_spectronaut_cli()
