"""Underlying function for processing of Gel Bands"""
import argparse
import os
import subprocess
from pathlib import Path
import shutil
import copy
import time
import sys
import fnmatch
import pandas as pd
from lxml import etree
import psutil

class MaxQuantHandler:
    def __init__(self, stop_queue, progress_queue,
                 MQ_version, MQ_path, db_map,
                 fasta_folder, output_folder, conditions, dbs, user_input_params,
                 raw_folder=None, job_name=None,
                 prot_quantification="Razor + Unique", num_missed_cleavages=2,
                 id_parse_rule=r">.*\|(.*)\|", desc_parse_rule=r">(.*)", andromeda_path=r"C:\Temp\Andromeda",
                 fixed_mods=["Carbamidomethyl (C)"], enzymes=["Trypsin/P"], use_enzyme_first_search_str="True",
                 fs_enzymes=["Trypsin/P"], var_mods=["Oxidation (M)", "Acetyl (Protein N-term)"],
                 second_peptide_str="False", match_between_runs="False", num_threads=16,
                 MQ_params=None,
                 run_mode="interactive"):

        progress_queue.put("Initializing MaxQuant handler")

        self.stop_queue = stop_queue
        self.progress_queue = progress_queue
        # Program paths
        self.MQ_path = MQ_path
        self.MQ_version = MQ_version
        self.validate_MQ_path()
        self.dotnet_path = None
        self.database_map = db_map

        self.fasta_folder = fasta_folder
        self.output_folder = output_folder
        self.job_name = job_name if job_name else str(Path(self.output_folder).name)

        self.conditions = conditions
        self.dbs = dbs

        self.prot_quantification = prot_quantification
        self.num_missed_cleavages = num_missed_cleavages
        self.fixed_mods = fixed_mods
        self.enzymes = enzymes
        self.use_enzyme_first_search_str = use_enzyme_first_search_str
        self.fs_enzymes = fs_enzymes
        self.var_mods = var_mods
        self.second_peptide_str = second_peptide_str
        self.match_between_runs = match_between_runs
        self.num_threads = num_threads
        self.id_parse_rule = id_parse_rule
        self.desc_parse_rule = desc_parse_rule
        self.andromeda_path = andromeda_path
        # Advanced
        self.user_input_params = user_input_params
        self.MQ_params = MQ_params
        # Run mode
        self.run_mode = run_mode

        self.error_flag = False
        self.stop_requested = False

        # Define file extensions
        self.fasta_extensions = ['.fa', '.FA', '.fasta', '.FASTA', '.faa', '.FAA']

        self.sentinel_file_path = Path(self.output_folder) / "stop_requested.sentinel"
        self.conditions_dict = self.make_conditions_dict()
        self.raw_folder = raw_folder
        self.get_common_parent_directory()
        self.species_dict = self.load_species_dict()
        self.database_paths = self.get_species_filepaths()

        db_string = '_'.join(self.dbs)
        self.master_fasta = Path(self.fasta_folder) / 'merged_fasta_folder' / f'custom_fasta_merged_w_{db_string.upper()}_database.fasta'
        self.MQ_op_folder = Path(self.raw_folder) / "combined/"
        self.temp_op_folder = Path(self.output_folder) / 'TEMP'
        self.temp_op_folder.mkdir(exist_ok=True)
        self.fasta_files = self.get_files_with_extensions()

        self.AppData_path = str(self.get_AppData_path())

    def validate_MQ_path(self):
        if not os.path.exists(self.MQ_path):
            raise FileNotFoundError(f"The provided MaxQuant path {self.MQ_path} does not exist.")
        if not os.path.isfile(self.MQ_path):
            raise ValueError(f"The provided MaxQuant path {self.MQ_path} is not a file.")

    def get_AppData_path(self):
        current_user_profile = os.getenv('USERPROFILE')
        if current_user_profile is None:
            return None
        appdata_folder = os.path.join(current_user_profile, 'AppData')
        return appdata_folder if os.path.exists(appdata_folder) else None

    def make_results_folder(self):
        results_op = Path(self.output_folder) / f"{self.job_name}_CNIO_prot_core_results"
        os.makedirs(results_op, exist_ok=True)
        return Path(results_op)

    def sentinel_file_exists(self):
        return os.path.exists(self.sentinel_file_path)

    def make_conditions_dict(self):
        try:
            ext = os.path.splitext(str(Path(self.conditions)))[1]
            if ext == ".xlsx":
                conditions_df = pd.read_excel(Path(self.conditions), sheet_name="Sheet1", dtype=str)
            elif ext in [".txt", ".tsv"]:
                conditions_df = pd.read_csv(Path(self.conditions), sep="\t", dtype=str)
            else:
                self.progress_queue.put("ERROR: Conditions file must be in .xlsx, .txt, or .tsv format")
                return None
        except Exception as e:
            self.progress_queue.put(f"ERROR: Failed to parse conditions file - {str(e)}")
            return None

        conditions_df['Basename'] = conditions_df['Raw file path'].apply(lambda x: os.path.basename(str(Path(x))))
        self.converted_op = Path(self.output_folder) / "mzML_files"
        conditions_df['mzML file'] = conditions_df['Raw file path'].apply(lambda x: str(self.converted_op / (os.path.basename(os.path.splitext(x)[0]) + '.mzML')))

        conditions_dict = {row['Basename']: row.drop('Basename').to_dict() for _, row in conditions_df.iterrows()}
        return conditions_dict

    def load_species_dict(self):
        df = pd.read_excel(self.database_map, sheet_name="Sheet1")
        return {row['Species'].upper(): row['Path'] for _, row in df.iterrows()}

    def get_species_filepaths(self):
        return [Path(self.species_dict.get(species.upper())) for species in self.dbs if species.upper() in self.species_dict]

    def get_common_parent_directory(self):
        if not self.raw_folder:
            parent_dirs = {Path(values['Raw file path']).parent for values in self.conditions_dict.values()}
            if len(parent_dirs) == 1:
                self.raw_folder = parent_dirs.pop()
            else:
                raise ValueError("Multiple parent directories found!")

    def get_files_with_extensions(self):
        folder = Path(self.fasta_folder)
        files = set()
        for ext in self.fasta_extensions:
            files.update(folder.glob(f'*{ext}'))
        return list(files)

    def show_error_message(self, message):
        self.progress_queue.put(f"RECOVERABLE ERROR: {message}")
        self.error_flag = True

    def check_stop_queue(self):
        if not self.stop_queue.empty():
            message = self.stop_queue.get()
            if message.startswith("STOP"):
                self.stop_requested = True
                self.progress_queue.put("UPDATE: Stop requested")

    def terminate_process_tree(self, pid):
        try:
            parent = psutil.Process(pid)
            for child in parent.children(recursive=True):
                child.kill()
            parent.kill()
        except psutil.NoSuchProcess:
            pass

    def terminate_external_processes(self, subprocess_handle):
        self.check_stop_queue()
        if subprocess_handle and self.stop_requested:
            self.progress_queue.put(f"UPDATE: Terminating external process with id {subprocess_handle.pid}")
            self.terminate_process_tree(subprocess_handle.pid)

    def cleanup_directory(self, directory_path):
        try:
            dir_path_obj = Path(directory_path)
            if dir_path_obj.exists() and dir_path_obj.is_dir():
                shutil.rmtree(dir_path_obj)
                return True
            return dir_path_obj.exists()
        except Exception as e:
            self.progress_queue.put(f"ERROR: Failed to remove {directory_path}: {e}")
            return False

    def cleanup_file(self, file_path):
        try:
            file_path_obj = Path(file_path)
            if file_path_obj.exists() and file_path_obj.is_file():
                file_path_obj.unlink()
                return True
            return False
        except Exception as e:
            self.progress_queue.put(f"ERROR: Failed to remove {file_path}: {e}")
            return False

    def check_and_terminate_if_sentinel_exists(self):
        self.check_stop_queue()
        if self.stop_requested or self.error_flag:
            self.progress_queue.put("UPDATE: GelBandIDo encountered\n an error" if self.error_flag else "UPDATE: Cancelling GelBandIDo")
            if hasattr(self, 'results_op'):
                self.cleanup_directory(self.results_op)
            if hasattr(self, 'temp_op_folder'):
                self.cleanup_directory(self.temp_op_folder)
            if hasattr(self, 'win_MQ_params_updated'):
                self.cleanup_file(str(self.win_MQ_params_updated))
            if hasattr(self, 'temp_MQ_params'):
                self.cleanup_file(self.temp_MQ_params)
            if hasattr(self, 'MQ_op_folder') and self.MQ_op_folder.exists():
                run_times_file = self.MQ_op_folder / 'proc' / '#runningTimes.txt'
                try:
                    with open(run_times_file, 'r') as f:
                        if "Finish writing tables" not in f.readlines()[-1].strip():
                            self.cleanup_directory(self.MQ_op_folder)
                except FileNotFoundError:
                    if not (self.MQ_op_folder / 'txt' / 'allPeptides.txt').exists():
                        self.cleanup_directory(self.MQ_op_folder)

            self.progress_queue.put("PROCESS CANCELLED: GelBandIDo cancelled due to " + ("unrecoverable errors" if self.error_flag else "user request"))
            sys.exit()

    def get_dotnet_path(self, search_folders):
        for folder in search_folders:
            for root, dirs, files in os.walk(folder):
                dirs[:] = [d for d in dirs if d not in ["Users", "$Recycle.Bin", "Windows", "ProgramData", "Spectronaut", "KNIME"]]
                dirs[:] = [d for d in dirs if not any(fnmatch.fnmatch(d, pat) for pat in ['*Spectronaut*', '*KNIME*'])]
                if "dotnet.exe" in files:
                    self.dotnet_path = os.path.join(root, "dotnet.exe")
                    return self.dotnet_path

    def concatenate_fasta_files(self):
        self.progress_queue.put("STARTING: Concatenating input and database fasta.")
        try:
            Path(self.master_fasta).parent.mkdir(parents=True, exist_ok=True)
            if os.path.exists(self.master_fasta):
                self.progress_queue.put("UPDATE: Merged fasta already exists--skipping")
            else:
                seen_identifiers = set()
                with Path(self.master_fasta).open('w') as output_file:
                    for fasta_file in self.fasta_files + self.database_paths:
                        with Path(fasta_file).open() as input_file:
                            write_sequence = False
                            for line in input_file:
                                if line.startswith('>'):
                                    identifier = line.strip()
                                    if identifier not in seen_identifiers:
                                        seen_identifiers.add(identifier)
                                        write_sequence = True
                                    else:
                                        write_sequence = False
                                if write_sequence and line.strip():
                                    output_file.write(line if line.endswith('\n') else line + '\n')
            self.progress_queue.put("STEP COMPLETED: Files concatenated.")
        except Exception as e:
            self.progress_queue.put(f"ERROR: Failed to concatenate fasta files: {e}")
            self.error_flag = True

    def create_MaxQuant_par(self):
        self.progress_queue.put(f"STARTING: Creating template MaxQuant params file (v{self.MQ_version}).")
        try:
            if self.MQ_params:
                self.progress_queue.put("STEP COMPLETED: Using MaxQuant params file provided by user.")
                return
            new_MQ_params_name = f"mqpar_version_{self.MQ_version}.xml"
            self.MQ_params = Path(self.temp_op_folder) / new_MQ_params_name
            if self.MQ_params.exists():
                self.MQ_params.unlink()

            process = subprocess.Popen(
                [self.MQ_path, '--create', str(self.MQ_params)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                if ".NET Core" in stderr:
                    self.progress_queue.put("UPDATE: MQ couldn't find dotnet. Retrying with search.")
                    dotnet_path = self.get_dotnet_path(['D:\\Software', 'C:\\', self.AppData_path])
                    process = subprocess.Popen(
                        [dotnet_path, self.MQ_path, '--create', str(self.MQ_params)],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
                    stdout, stderr = process.communicate()
                if process.returncode != 0:
                    self.progress_queue.put(f"ERROR: MaxQuant --create failed: {stderr}")
                    self.error_flag = True
                    return
            self.progress_queue.put("STEP COMPLETED: Created template MaxQuant params file.")
        except Exception as e:
            self.progress_queue.put(f"ERROR: Failed to create MaxQuant params file: {e}")
            self.error_flag = True

    def edit_MQ_par(self):
        self.progress_queue.put("STARTING: Editing MaxQuant params file.")
        try:
            tree = etree.parse(self.MQ_params)
            root = tree.getroot()

            # Edit fasta location
            fastaFile_block = root.find("./fastaFiles")
            for child in list(fastaFile_block)[1:]:
                fastaFile_block.remove(child)
            FastaFileInfo = fastaFile_block.find("FastaFileInfo")
            fasta_path = FastaFileInfo.find('fastaFilePath')
            fasta_path.text = os.path.basename(self.master_fasta)

            # Edit raw files and attributes
            branches = {name: root.find(f"./{name}") for name in ["filePaths", "experiments", "fractions", "ptms", "paramGroupIndices", "referenceChannel"]}
            for branch in branches.values():
                for child in list(branch)[1:]:
                    branch.remove(child)

            child_tags = ['string', 'string', 'short', 'boolean', 'int', 'string']
            children = [branches[name].find(tag) for tag, name in zip(child_tags, branches.keys())]

            ET_raw_files = list(self.conditions_dict.keys())
            experiments_list = [details['Experiment'] for details in self.conditions_dict.values()]
            common_length = len(self.conditions_dict)
            par_text_lists = [ET_raw_files, experiments_list, ['32767'] * common_length, ['False'] * common_length, ['0'] * common_length, [''] * common_length]

            for i, new_par_texts in enumerate(zip(*par_text_lists)):
                for par_child, new_par_text in zip(children, new_par_texts):
                    if i > 0:
                        new_dupe = copy.deepcopy(par_child)
                        new_dupe.text = new_par_text
                        branches[list(branches.keys())[children.index(par_child)]].append(new_dupe)
                    else:
                        par_child.text = new_par_text

            root.find("./writeAllPeptidesTable").text = "True"

            if not self.user_input_params:
                quant_mode_branch = root.find("./quantMode")
                quant_mode_branch.text = {'All': '0', 'Razor + Unique': '1', 'Unique': '2'}[self.prot_quantification]

                for param_group in root.findall("./parameterGroups/parameterGroup"):
                    param_group.find("maxMissedCleavages").text = str(self.num_missed_cleavages)
                    for branch, mods in [("fixedModifications", self.fixed_mods), ("enzymes", self.enzymes), ("enzymesFirstSearch", self.fs_enzymes), ("variableModifications", self.var_mods)]:
                        mod_branch = param_group.find(branch)
                        for child in list(mod_branch):
                            mod_branch.remove(child)
                        for mod in mods:
                            etree.SubElement(mod_branch, "string").text = mod
                    param_group.find("useEnzymeFirstSearch").text = self.use_enzyme_first_search_str

                FastaFileInfo.find("identifierParseRule").text = self.id_parse_rule
                FastaFileInfo.find("descriptionParseRule").text = self.desc_parse_rule
                root.find("./fixedSearchFolder").text = self.andromeda_path
                root.find("./secondPeptide").text = self.second_peptide_str
                root.find("./matchBetweenRuns").text = self.match_between_runs
                root.find("./numThreads").text = str(self.num_threads)

            file_str = Path(self.MQ_params).name.replace('.xml', '_temp.xml')
            self.temp_MQ_params = Path(self.temp_op_folder) / file_str
            etree.ElementTree(root).write(str(self.temp_MQ_params), pretty_print=True)
            self.progress_queue.put("STEP COMPLETED: Edited MaxQuant params file.")
        except Exception as e:
            self.progress_queue.put(f"ERROR: Failed to edit MaxQuant params file: {e}")
            self.error_flag = True

    def run_MaxQuant(self):
        self.progress_queue.put(f"STARTING: Running MaxQuant version {self.MQ_version}.")
        file_name = Path(self.temp_MQ_params).name.replace('_temp.xml', '_updated.xml')
        self.win_MQ_params_updated = Path(self.output_folder) / file_name
        master_folder = str(Path(self.master_fasta).parent)

        if not self.MQ_op_folder.exists():
            if self.win_MQ_params_updated.exists():
                self.win_MQ_params_updated.unlink()

            process = subprocess.Popen(
                [self.MQ_path, str(self.temp_MQ_params), '--changeFolder', str(self.win_MQ_params_updated), master_folder, str(self.raw_folder)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
            stdout, stderr = process.communicate()
            if process.returncode != 0 and ".NET Core" in stderr:
                self.dotnet_path = self.get_dotnet_path(['D:\\Software', 'C:\\', self.AppData_path])
                process = subprocess.Popen(
                    [self.dotnet_path, self.MQ_path, str(self.temp_MQ_params), '--changeFolder', str(self.win_MQ_params_updated), master_folder, str(self.raw_folder)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
                stdout, stderr = process.communicate()

            if process.returncode == 0:
                self.progress_queue.put("STEP COMPLETED: Converted xml. Ready to begin MaxQuant.")
            else:
                self.progress_queue.put(f"ERROR: Failed preparing for MaxQuant: {stderr}")
                self.error_flag = True
                return

            command_list = [self.dotnet_path or self.MQ_path, str(self.win_MQ_params_updated)]
            process = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)

            while process.poll() is None:
                self.check_stop_queue()
                if self.stop_requested:
                    self.terminate_external_processes(process)
                    break
                time.sleep(2)

            stdout, stderr = process.communicate()
            if process.returncode == 0:
                self.progress_queue.put(f"STEP COMPLETED: Ran MaxQuant version {self.MQ_version}.")
            else:
                self.progress_queue.put(f"ERROR: MaxQuant version {self.MQ_version} completed with error: {stderr}")
                self.cleanup_directory(self.MQ_op_folder)
                self.error_flag = True
        else:
            self.progress_queue.put("STEP COMPLETED: Found existing MaxQuant result--skipping run.")

    def run_MaxQuant_cli(self):
        try:
            self.check_and_terminate_if_sentinel_exists()
            self.concatenate_fasta_files()
            self.check_and_terminate_if_sentinel_exists()
            self.create_MaxQuant_par()
            self.check_and_terminate_if_sentinel_exists()
            self.edit_MQ_par()
            self.check_and_terminate_if_sentinel_exists()
            self.run_MaxQuant()
            self.check_and_terminate_if_sentinel_exists()
            if hasattr(self, 'temp_op_folder'):
                self.cleanup_directory(self.temp_op_folder)
            self.progress_queue.put("PROCESS COMPLETED: MaxQuant completed.")
        except Exception as e:
            self.progress_queue.put(f"ERROR: Process failed: {e}")
            self.error_flag = True
            self.check_and_terminate_if_sentinel_exists()

# Utility functions remain unchanged
def comma_separated_string_to_list(comma_separated_string):
    return comma_separated_string.split(',') if comma_separated_string else None

def str_to_bool(v):
    if isinstance(v, bool):
        return v
    return v.lower() in ('yes', 'true', 't', 'y', '1')

def str_to_int(v):
    if isinstance(v, int):
        return v
    try:
        return int(v)
    except ValueError:
        raise argparse.ArgumentTypeError('Numerical value expected.')

def str_to_float(v):
    if isinstance(v, float):
        return v
    try:
        return float(v)
    except ValueError:
        raise argparse.ArgumentTypeError('Numerical value expected.')