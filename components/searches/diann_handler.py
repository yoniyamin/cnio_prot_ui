import time
import subprocess
from pathlib import Path
import os
import psutil
import multiprocessing
from queue import Queue
import pandas as pd
import sys
import threading


class DIANNHandler:
    def __init__(self,
                 diann_exe,
                 fasta,
                 conditions,
                 op_folder,
                 progress_callback=None,
                 msconvert_path=None,
                 max_missed_cleavage="1",
                 max_var_mods="2",
                 NtermMex_mod=True,
                 CCarb_mod=True,
                 OxM_mod=True,
                 AcNterm_mod=False,
                 Phospho_mod=False,
                 KGG_mod=False,
                 peptide_length_range_min="7",
                 peptide_length_range_max="30",
                 precursor_charge_range_min="2",
                 precursor_charge_range_max="4",
                 precursor_min="390",
                 precursor_max="1050",
                 fragment_min="200",
                 fragment_max="1800",
                 threads="20",
                 MBR=False):
        """
        Initialize the DIA-NN handler

        Args:
            diann_exe: Path to DIA-NN executable
            fasta: Path to FASTA file
            conditions: Path to conditions file or dictionary of conditions
            op_folder: Output folder path
            progress_callback: Function to call with progress updates
            msconvert_path: Path to MSConvert executable
            max_missed_cleavage: Maximum number of missed cleavages
            max_var_mods: Maximum number of variable modifications
            NtermMex_mod: Whether to enable N-terminal methionine excision
            CCarb_mod: Whether to enable carbamidomethylation of cysteine
            OxM_mod: Whether to enable oxidation of methionine
            AcNterm_mod: Whether to enable N-terminal acetylation
            Phospho_mod: Whether to enable phosphorylation
            KGG_mod: Whether to enable K-GG modification
            peptide_length_range_min: Minimum peptide length
            peptide_length_range_max: Maximum peptide length
            precursor_charge_range_min: Minimum precursor charge
            precursor_charge_range_max: Maximum precursor charge
            precursor_min: Minimum precursor m/z
            precursor_max: Maximum precursor m/z
            fragment_min: Minimum fragment m/z
            fragment_max: Maximum fragment m/z
            threads: Number of threads to use
            MBR: Whether to enable match between runs
        """
        self.diann_exe = diann_exe
        self.use_mzML = True if msconvert_path else False
        self.msconvert_path = msconvert_path
        self.op_folder = op_folder
        self.conditions = conditions
        self.progress_callback = progress_callback
        self.conditions_dict = {}
        self.stop_requested = False

        # DIA-NN parameters
        self.max_missed_cleavage = max_missed_cleavage
        self.max_var_mods = max_var_mods
        self.NtermMex_mod = NtermMex_mod
        self.CCarb_mod = CCarb_mod
        self.OxM_mod = OxM_mod
        self.AcNterm_mod = AcNterm_mod
        self.Phospho_mod = Phospho_mod
        self.KGG_mod = KGG_mod
        self.peptide_length_range_min = peptide_length_range_min
        self.peptide_length_range_max = peptide_length_range_max
        self.precursor_charge_range_min = precursor_charge_range_min
        self.precursor_charge_range_max = precursor_charge_range_max
        self.precursor_min = precursor_min
        self.precursor_max = precursor_max
        self.fragment_min = fragment_min
        self.fragment_max = fragment_max
        self.threads = threads
        self.MBR = MBR

        # Setup the diann plotter path
        diann_parent = Path(self.diann_exe).parent.absolute()
        self.diann_plotter = Path(diann_parent) / 'dia-nn-plotter.exe'

        # Setup the mzML folder
        self.mzml_folder = Path(self.op_folder) / 'mzML_folder'
        self.fasta = fasta
        self.predicted_library = None

        # Setup the output files
        self.diann_config = None
        self.sentinel_file = None
        self.log_file = None
        self.error_flag = False

    def log_progress(self, message):
        """Log progress to the callback or print to console"""
        if self.progress_callback:
            self.progress_callback(message)
        print(message)

    def make_output_folder(self):
        """Create the output folder if it doesn't exist"""
        if not Path(self.op_folder).exists():
            Path(self.op_folder).mkdir(parents=True, exist_ok=True)
            self.log_progress(f"Created output folder: {self.op_folder}")

    def make_output_files(self):
        """Setup output file paths"""
        self.diann_config = Path(self.op_folder) / "diann_config.txt"
        self.sentinel_file = Path(self.op_folder) / "stop.sentinel"
        self.log_file = Path(self.op_folder) / "runtime_log.log"
        self.log_progress(f"Output files will be created in: {self.op_folder}")

    def make_conditions_dict(self):
        """Parse the conditions file into a dictionary"""
        conditions_df = None
        self.log_progress(f"Processing conditions file: {self.conditions}")

        if isinstance(self.conditions, str):
            try:
                ext = os.path.splitext(str(Path(self.conditions)))[1]
                if ext == ".xlsx":
                    conditions_df = pd.read_excel(Path(self.conditions), sheet_name="Sheet1", dtype=str)
                elif ext in [".txt", ".tsv", ".csv"]:
                    sep = "," if ext == ".csv" else "\t"
                    conditions_df = pd.read_csv(Path(self.conditions), sep=sep, dtype=str)
            except Exception as e:
                self.log_progress(f"Error reading conditions file: {e}")
                self.error_flag = True
                return None
        elif isinstance(self.conditions, dict):
            conditions_df = pd.DataFrame(self.conditions.values(), columns=['Raw file'])

        if conditions_df is None:
            self.log_progress("Error: No valid conditions provided")
            self.error_flag = True
            return None

        # Add a 'Basename' column to the dataframe
        conditions_df['Basename'] = conditions_df['Raw file'].apply(lambda x: Path(x).stem)

        # Add column for full raw file paths if they don't already have full paths
        if not all(str(Path(x)).startswith(('/', 'C:', 'D:')) for x in conditions_df['Raw file']):
            # These are just filenames, not full paths
            self.log_progress("Converting relative paths to absolute paths")
            raw_folder = Path(self.op_folder) / "raw_files"
            raw_folder.mkdir(exist_ok=True, parents=True)
            conditions_df['Full raw path'] = conditions_df['Raw file'].apply(
                lambda x: str(raw_folder / Path(x).name)
            )
        else:
            # These are already full paths
            conditions_df['Full raw path'] = conditions_df['Raw file']

        # Add column for mzML files
        conditions_df['Full mzML path'] = conditions_df['Basename'].apply(
            lambda x: str(Path(self.mzml_folder) / f'{x}.mzML')
        )

        # Convert the dataframe to a dictionary with 'basename' as keys
        self.conditions_dict = {}
        for _, row in conditions_df.iterrows():
            basename = row['Basename']
            self.conditions_dict[basename] = row.to_dict()

        self.log_progress(f"Processed {len(self.conditions_dict)} samples from conditions file")
        return True

    def check_for_stop_signal(self, subprocess_handle=None):
        """Check if a stop was requested and handle accordingly"""
        if Path(self.sentinel_file).exists():
            self.stop_requested = True
            self.log_progress("PROCESS CANCELLED: DIA-NN handler cancelled.")

            if subprocess_handle:
                self.terminate_external_processes(subprocess_handle)

            return True

        if self.error_flag:
            self.log_progress("ERROR: DIA-NN handler cancelled due to errors.")

            if subprocess_handle:
                self.terminate_external_processes(subprocess_handle)

            return True

        return False

    def terminate_external_processes(self, subprocess_handle):
        """Terminate an external process and all its children"""
        if subprocess_handle:
            self.log_progress(f"Terminating external process with id {subprocess_handle.pid}")
            try:
                parent = psutil.Process(subprocess_handle.pid)
                # Terminate children first
                for child in parent.children(recursive=True):
                    child.kill()
                # Then terminate parent
                parent.kill()
            except psutil.NoSuchProcess:
                self.log_progress(f"Process with PID {subprocess_handle.pid} no longer exists.")
            except Exception as e:
                self.log_progress(f"Error terminating process: {e}")

    def run_msconvert(self):
        """Convert raw files to mzML format using MSConvert"""
        if not self.use_mzML or not self.msconvert_path:
            self.log_progress("Skipping MSConvert step (no path provided or mzML conversion disabled)")
            return True

        self.log_progress("STARTING: Converting to mzML format for plotting.")

        # Create mzML folder if it doesn't exist
        if not self.mzml_folder.exists():
            self.mzml_folder.mkdir(parents=True)

        # Check if all mzML files already exist
        mzML_files = [details['Full mzML path'] for details in self.conditions_dict.values()]

        if all(Path(mzML).exists() for mzML in mzML_files):
            self.log_progress("STEP COMPLETED: All mzML files already exist. No conversion needed.")
            return True

        # If not all mzML files exist, check if all raw files exist
        raw_files = [details['Full raw path'] for details in self.conditions_dict.values()]
        missing_raw_files = [raw for raw in raw_files if not Path(raw).exists()]

        if missing_raw_files:
            self.log_progress(f"ERROR: {len(missing_raw_files)} raw files are missing: {missing_raw_files[:3]}")
            self.error_flag = True
            return False

        # All raw files exist, convert them to mzML
        try:
            BASE_COMMAND = [
                "--zlib",
                "--outdir", f'{str(self.mzml_folder)}',
                "--filter", "peakPicking vendor msLevel=1-",
                "--filter",
                "titleMaker <RunId>.<ScanNumber>.<ScanNumber>.<ChargeState> File:\"\"\"^<SourcePath^>\"\"\", NativeID:\"\"\"^<Id^>\"\"\""
            ]

            command = [str(Path(self.msconvert_path))] + BASE_COMMAND + raw_files

            self.log_progress(f"Running MSConvert: {' '.join(command[:5])}...")

            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True, bufsize=1, universal_newlines=True) as msconvert_process:

                # Monitor the process
                while msconvert_process.poll() is None:
                    # Check for stop signal every 5 seconds
                    if self.check_for_stop_signal(msconvert_process):
                        return False

                    time.sleep(5)

                # Get final output
                stdout, stderr = msconvert_process.communicate()

                if msconvert_process.returncode == 0:
                    self.log_progress("STEP COMPLETED: Files converted to mzML format.")
                    return True
                else:
                    self.log_progress(f"ERROR: MSConvert failed with return code {msconvert_process.returncode}")
                    self.log_progress(f"Stderr: {stderr}")
                    self.error_flag = True
                    return False

        except Exception as e:
            self.log_progress(f"ERROR: Exception during MSConvert: {str(e)}")
            self.error_flag = True
            return False

    def write_diann_command(self):
        """Generate the DIA-NN command configuration file"""
        self.log_progress("Preparing DIA-NN configuration")

        # Get input files
        if self.use_mzML:
            input_files = [str(file) for file in Path(self.mzml_folder).glob("*.mzML")]
            if not input_files:
                self.log_progress("ERROR: No mzML files found")
                self.error_flag = True
                return False
        else:
            input_files = [details['Full raw path'] for details in self.conditions_dict.values()]

        # Define output paths
        report_file = Path(self.op_folder) / 'report.tsv'
        lib_file = Path(self.op_folder) / 'report-lib.tsv'
        fasta_file = Path(self.fasta)

        # Start building the command list
        command = []

        # Add each input file with the --f flag
        for input_file in input_files:
            command.extend(['--f', input_file])

        # Add library generation options
        if self.predicted_library:
            command.extend(['--lib', str(Path(self.predicted_library))])
        else:
            command.extend(['--lib', '--gen-spec-lib', '--predictor'])

        # Add common parameters
        command.extend([
            '--threads', str(self.threads),
            '--verbose', '1',
            '--out', str(report_file),
            '--qvalue', '0.01',
            '--matrices',
            '--out-lib', str(lib_file),
            '--fasta', str(fasta_file),
            '--fasta-search',
            '--min-fr-mz', str(self.fragment_min),
            '--max-fr-mz', str(self.fragment_max),
        ])

        # Add optional modifications
        if self.NtermMex_mod:
            command.extend(['--met-excision'])

        # Add peptide and precursor parameters
        command.extend([
            '--min-pep-len', str(self.peptide_length_range_min),
            '--max-pep-len', str(self.peptide_length_range_max),
            '--min-pr-mz', str(self.precursor_min),
            '--max-pr-mz', str(self.precursor_max),
            '--min-pr-charge', str(self.precursor_charge_range_min),
            '--max-pr-charge', str(self.precursor_charge_range_max),
            '--cut', 'K*,R*',
            '--missed-cleavages', str(self.max_missed_cleavage),
        ])

        # Add fixed modifications
        if self.CCarb_mod:
            command.extend(['--unimod4'])

        # Add variable modifications
        command.extend(['--var-mods', str(self.max_var_mods)])

        if self.OxM_mod:
            command.extend(['--var-mod', 'UniMod:35,15.994915,M'])

        if self.AcNterm_mod:
            command.extend(['--var-mod', 'UniMod:1,42.010565,*n', '--monitor-mod', 'UniMod:1'])

        if self.Phospho_mod:
            command.extend(['--var-mod', 'UniMod:21,79.966331,STY', '--monitor-mod', 'UniMod:21'])

        if self.KGG_mod:
            command.extend(['--var-mod', 'UniMod:121,114.042927,K', '--monitor-mod', 'UniMod:121', '--no-cut-after-mod',
                            'UniMod:121'])

        # Add match between runs if enabled
        if self.MBR:
            command.extend(['--reanalyse'])

        # Add final options
        command.extend(['--relaxed-prot-inf', '--rt-profiling'])

        # Write the command to the config file
        with open(self.diann_config, 'w') as config_handle:
            config_string = ' '.join(command)
            config_handle.write(config_string)
            self.log_progress(f"DIA-NN configuration written to {self.diann_config}")

        return True

    def build_diann_command(self):
        """Build the DIA-NN command with the config file"""
        return [self.diann_exe, '--cfg', str(self.diann_config)]

    def run_diann(self):
        """Run the DIA-NN search"""
        try:
            self.log_progress("STARTING PROCESS: Running DIA-NN search")

            # Write the DIA-NN command configuration
            if not self.write_diann_command():
                return False

            # Build the command
            command_list = self.build_diann_command()
            self.log_progress(f"Running DIA-NN command: {command_list}")

            # Start the subprocess
            diann_process = subprocess.Popen(
                command_list,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            # Create a log file for the output
            log_path = Path(self.op_folder) / "diann_log.txt"
            with open(log_path, 'w') as log_file:
                # Monitor the process
                while diann_process.poll() is None:
                    # Read from stdout
                    output = diann_process.stdout.readline()
                    if output:
                        output = output.strip()
                        self.log_progress(f"DIA-NN: {output}")
                        log_file.write(f"{output}\n")
                        log_file.flush()

                    # Check for stop signal every few seconds
                    if self.check_for_stop_signal(diann_process):
                        return False

                    time.sleep(0.1)

                # Get any remaining output
                stdout, stderr = diann_process.communicate()
                if stdout:
                    log_file.write(stdout)
                if stderr:
                    log_file.write(f"\nERRORS:\n{stderr}")

            if diann_process.returncode == 0:
                self.log_progress("STEP COMPLETED: DIA-NN search completed successfully")
                return True
            else:
                self.log_progress(f"ERROR: DIA-NN search failed with return code {diann_process.returncode}")
                self.error_flag = True
                return False

        except Exception as e:
            self.log_progress(f"ERROR: Exception during DIA-NN search: {str(e)}")
            self.error_flag = True
            return False

    def run_diann_plotter(self):
        """Run the DIA-NN plotter to generate visualization reports"""
        # Check if the plotter executable exists
        if not Path(self.diann_plotter).exists():
            self.log_progress("WARNING: DIA-NN plotter not found, skipping visualization")
            return True

        self.log_progress("STARTING: Running DIA-NN plotter for visualization")

        try:
            report_tsv = Path(self.op_folder) / 'report.tsv'
            stats_file = Path(self.op_folder) / 'report.stats.tsv'
            pdf_output = Path(self.op_folder) / 'report.pdf'

            if not report_tsv.exists():
                self.log_progress("ERROR: DIA-NN report file not found, can't run plotter")
                return False

            command_list = [str(self.diann_plotter), str(stats_file), str(report_tsv), str(pdf_output)]
            self.log_progress(f"Running DIA-NN plotter: {command_list}")

            plotter_process = subprocess.Popen(
                command_list,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Monitor the process
            while plotter_process.poll() is None:
                # Check for stop signal
                if self.check_for_stop_signal(plotter_process):
                    return False

                time.sleep(1)

            # Check the return code
            if plotter_process.returncode == 0:
                self.log_progress("STEP COMPLETED: DIA-NN plotter finished successfully")
                return True
            else:
                stdout, stderr = plotter_process.communicate()
                self.log_progress(f"ERROR: DIA-NN plotter failed with return code {plotter_process.returncode}")
                self.log_progress(f"Stderr: {stderr}")
                return False

        except Exception as e:
            self.log_progress(f"ERROR: Exception during DIA-NN plotter: {str(e)}")
            return False

    def run_workflow(self):
        """Run the complete DIA-NN workflow"""
        try:
            # Setup
            self.make_output_folder()
            self.make_output_files()

            # Process conditions file
            if not self.make_conditions_dict():
                return False

            # Run MSConvert if needed
            if self.use_mzML:
                if not self.run_msconvert():
                    return False

            # Run DIA-NN search
            if not self.run_diann():
                return False

            # Run DIA-NN plotter
            self.run_diann_plotter()

            self.log_progress("PROCESS COMPLETED: DIA-NN analysis workflow finished successfully")
            return True

        except Exception as e:
            self.log_progress(f"ERROR: Unhandled exception in DIA-NN workflow: {str(e)}")
            return False


def launch_diann_job(job_data, progress_callback=None):
    """
    Launch a DIA-NN job in a separate thread

    Args:
        job_data: Dictionary containing all the job parameters
        progress_callback: Function to call with progress updates
    """
    try:
        # Extract parameters from job_data
        diann_path = job_data.get('diann_path')
        fasta_file = job_data.get('fasta_file')
        conditions_file = job_data.get('conditions_file')
        output_folder = job_data.get('output_folder')
        msconvert_path = job_data.get('msconvert_path')

        # Extract DIA-NN parameters
        params = {
            'max_missed_cleavage': job_data.get('missed_cleavage', '1'),
            'max_var_mods': job_data.get('max_var_mods', '2'),
            'NtermMex_mod': job_data.get('mod_nterm_m_excision', True),
            'CCarb_mod': job_data.get('mod_c_carb', True),
            'OxM_mod': job_data.get('mod_ox_m', True),
            'AcNterm_mod': job_data.get('mod_ac_nterm', False),
            'Phospho_mod': job_data.get('mod_phospho', False),
            'KGG_mod': job_data.get('mod_k_gg', False),
            'peptide_length_range_min': job_data.get('peptide_length_min', '7'),
            'peptide_length_range_max': job_data.get('peptide_length_max', '30'),
            'precursor_charge_range_min': job_data.get('precursor_charge_min', '2'),
            'precursor_charge_range_max': job_data.get('precursor_charge_max', '4'),
            'precursor_min': job_data.get('precursor_min', '390'),
            'precursor_max': job_data.get('precursor_max', '1050'),
            'fragment_min': job_data.get('fragment_min', '200'),
            'fragment_max': job_data.get('fragment_max', '1800'),
            'threads': job_data.get('threads', '20'),
            'MBR': job_data.get('mbr', False),
        }

        # Log initial message
        if progress_callback:
            progress_callback(f"Starting DIA-NN job with output to {output_folder}")

        # Create handler and run workflow
        handler = DIANNHandler(
            diann_exe=diann_path,
            fasta=fasta_file,
            conditions=conditions_file,
            op_folder=output_folder,
            msconvert_path=msconvert_path,
            progress_callback=progress_callback,
            **params
        )

        result = handler.run_workflow()

        # Final status update
        if progress_callback:
            if result:
                progress_callback("DIA-NN job completed successfully!")
            else:
                progress_callback("DIA-NN job failed. Check logs for details.")

        return result

    except Exception as e:
        if progress_callback:
            progress_callback(f"Error in DIA-NN job: {str(e)}")
        print(f"Error in DIA-NN job: {str(e)}")
        return False