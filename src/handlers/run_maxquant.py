"""Underlying function for processing of Gel Bands"""

import os
import subprocess
from pathlib import Path
import shutil
import copy
import time
import argparse
import sys
import tkinter as tk
from tkinter import messagebox
from getpass import getuser
import fnmatch
import pandas as pd
from lxml import etree
import psutil
# import openpyxl
#import fastparquet

#from GelBandIDo_summary_plots_class_jinja import GelBandIDoPlotter
#from Basepeak_plotter_jinja import *

class MaxQuant_handler:
    def __init__(self, stop_queue, progress_queue,
                 MQ_version, MQ_path, db_map,
                 fasta_folder, output_folder, conditions, dbs, user_input_params,
                 raw_folder = None, job_name = None,
                 prot_quantification="Razor + Unique", num_missed_cleavages=2,
                 id_parse_rule=r">.*\|(.*)\|",desc_parse_rule=r">(.*)",andromeda_path=r"C:\Temp\Andromeda",
                 fixed_mods=["Carbamidomethyl (C)"], enzymes=["Trypsin/P"], use_enzyme_first_search_str = "True",
                 fs_enzymes= ["Trypsin/P"], var_mods = ["Oxidation (M)", "Acetyl (Protein N-term)"],
                 second_peptide_str = "False", match_between_runs = "False", num_threads = 16,
                 MQ_params=None,
                 run_mode = "interactive"
                 ):

        progress_queue.put("I'm here too! Trying to initialize")

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
        self.num_missed_cleavages =  num_missed_cleavages
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
        # Extra POIs
        # MS chromatogram bp identifier display options
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

        db_string = ('_').join(self.dbs)
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
            return None  # Handle the case where USERPROFILE is not set

        appdata_folder = os.path.join(current_user_profile, 'AppData')
        if not os.path.exists(appdata_folder):
            return None
        else:
            return appdata_folder

    def make_results_folder(self):
        # Create results folder if it doesn't exist
        results_op = Path(self.output_folder) / f"{self.job_name}_CNIO_prot_core_results"
        if not os.path.exists(results_op):
            os.makedirs(results_op)
        return Path(results_op)

    def sentinel_file_exists(self):
        return os.path.exists(self.sentinel_file_path)

    def make_conditions_dict(self):
        try:
            ext = os.path.splitext(str(Path(self.conditions)))[1]
            if ext == ".xlsx":
                conditions_df = pd.read_excel(Path(self.conditions), sheet_name="Sheet1", dtype = str)
            elif ext == ".txt" or ext == ".tsv":
                conditions_df = pd.read_csv(Path(self.conditions), sep="\t", dtype = str)
            else:
                print("Please use a file in excel or tab-delimited format.")
                return None
        except IndexError as e:
            print(f"Error: {e}. Please use a file in excel or tab-delimited format.")
            return None
        
        # Add a 'Basename' column to the dataframe
        print(conditions_df)
        conditions_df['Basename'] = conditions_df['Raw file path'].apply(lambda x: os.path.basename(str(Path(x))))

        # Add column for mzML files
        self.converted_op = Path(self.output_folder) / "mzML_files"
        conditions_df['mzML file'] = conditions_df['Raw file path'].apply(lambda x: str(self.converted_op / str(os.path.basename(os.path.splitext(x)[0])+ '.mzML')))
        
        # Convert the dataframe to a dictionary with 'raw_file' as keys
        conditions_dict = {}
        for _, row in conditions_df.iterrows():
            raw_file = row['Basename']
            conditions_dict[raw_file] = row.drop('Basename').to_dict()
        return conditions_dict

    # Define function to map species to file location
    def load_species_dict(self):
        # Load the file into a dataframe
        df = pd.read_excel(self.database_map, sheet_name="Sheet1")
    
        # Convert species names to lowercase and create dictionary
        species_dict = {row['Species'].upper(): row['Path'] for _, row in df.iterrows()}
    
        return species_dict

    def get_species_filepaths(self):
        # Convert input to uppercase and fetch the filepath
        return [Path(self.species_dict.get(species.upper(), None)) for species in self.dbs]

    def get_common_parent_directory(self):
        if not self.raw_folder:
            parent_dirs = set()
            file_paths = [values['Raw file path'] for values in self.conditions_dict.values()]
            for file_path in file_paths:
                # get the parent directory name
                parent_dir = Path(file_path).parent
                parent_dirs.add(parent_dir)
                print(parent_dir)

            # check if all parent directory names are the same
            if len(parent_dirs) == 1:
                self.raw_folder = parent_dirs.pop()            
            else:
                raise ValueError("Multiple parent directories found!")

    def get_files_with_extensions(self):
        files = []
        folder=Path(self.fasta_folder)
        for extension in self.fasta_extensions:
            files.extend(folder.glob(f'*{extension}'))
        files=list(set(files))
        return files


    def show_error_message(self, message, retry_callback=None):
        if self.run_mode == "command_line":
            root = tk.Tk()
            root.withdraw()  # Hide the main window
            messagebox.showerror("Error", message)
            if retry_callback:
                retry = messagebox.askretrycancel("Retry", "Do you want to try again?")
                root.destroy()  # Destroy the root window to unblock
                if retry:
                    return retry_callback()  # Call the retry callback directly
            else:
                root.destroy()
        else:
            # Add recoverage error logic for interactive mode here
            self.progress_queue.put("RECOVERABLE ERROR: "+message)
            # For now, just exit
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
            print(f"Attempting to terminate parent process with PID: {parent.pid}")
            for child in parent.children(recursive=True):
                print(f"Forcefully killing child process with PID: {child.pid}")
                child.kill()
            print(f"Forcefully killing parent process with PID: {parent.pid}")
            parent.kill()
        except psutil.NoSuchProcess:
            print(f"Process with PID {pid} no longer exists.")

    def terminate_external_processes(self, subprocess_handle):
        self.check_stop_queue()
        if subprocess_handle and self.stop_requested:
            print(f"PID of subprocess to be terminated: {subprocess_handle.pid}")
            self.progress_queue.put("UPDATE: Terminating external process with id {subprocess_handle.pid} and all child processes.")
            self.terminate_process_tree(subprocess_handle.pid)
                
    def cleanup_directory(self, directory_path):
        def attempt_cleanup():
            dir_path_obj = Path(directory_path)
            try:
                if dir_path_obj.exists() and dir_path_obj.is_dir():
                    shutil.rmtree(dir_path_obj)
                    print(f"Removing folder and all files in {directory_path}.")
                    return True
                elif not dir_path_obj.exists():
                    print(f"Directory doesn't exist: {directory_path}")
                    return True
                    
            except PermissionError as e:
                error_message = f"Permission denied while removing {directory_path}: {e}"
                self.show_error_message(error_message, retry_callback=attempt_cleanup)
                print(error_message)
                return False
            except Exception as e:
                print(f"Error while removing {directory_path}: {e}")
                self.progress_queue.put(
                    f"ERROR: Error while removing {directory_path}: {e}"
                    )
                return False

        return attempt_cleanup()

    def cleanup_file(self, file_path):
        def attempt_cleanup():
            file_path_obj = Path(file_path)
            try:
                if file_path_obj.exists() and file_path_obj.is_file():
                    file_path_obj.unlink()
                    print(f"Removing file {file_path}.")
                    return True
            except PermissionError as e:
                error_message = f"Permission denied while removing file {file_path}: {e}"
                self.show_error_message(error_message, retry_callback=attempt_cleanup)
                print(error_message)
                return False
            except Exception as e:
                print(f"Error while removing file {file_path}: {e}")
                self.progress_queue.put(
                    f"ERROR:Error while removing {file_path}: {e}"
                    )
                return False

        return attempt_cleanup()

    def check_and_terminate_if_sentinel_exists(self):
        self.check_stop_queue()
        if self.stop_requested or self.error_flag:
        #if self.stop_requested:
            if self.error_flag:
                self.progress_queue.put("UPDATE: GelBandIDo encountered\n an error")
            else:
                self.progress_queue.put("UPDATE: Cancelling GelBandIDo")
            # Perform cleanup operations
            # Remove results folder and all contents
            if hasattr(self, 'results_op'):
                self.cleanup_directory(self.results_op)
                
            if hasattr(self, 'temp_op_folder'):
                self.cleanup_directory(self.temp_op_folder)

            # Remove updated mqpar file
            if hasattr(self, 'win_MQ_params_updated'):
                self.cleanup_file(str(self.win_MQ_params_updated))   

            # Remove temporary mqpar file
            if hasattr(self, 'temp_MQ_params'):
                self.cleanup_file(self.temp_MQ_params) 

            # Remove only UNFINISHED mqpar
            if hasattr(self, 'MQ_op_folder'):
                if Path(self.MQ_op_folder).exists():
                    run_times_file = Path(self.MQ_op_folder) / 'proc' / '#runningTimes.txt'
                    try:
                        with open(run_times_file, 'r') as f:
                            lines = f.readlines()
                            # Get the last line
                            last_line = lines[-1].strip()
                            # Check if the last line contains the string "Finish writing tables"
                            if "Finish writing tables" not in last_line:
                                self.cleanup_directory(self.MQ_op_folder)
                            else:
                                print("Found completed MaxQuant folder--not deleting.")
                    except FileNotFoundError:
                        if Path(self.MQ_op_folder) / 'txt' / 'allPeptides.txt' == False:
                            self.cleanup_directory(self.MQ_op_folder)
                        else:
                            print("Found completed MaxQuant folder--not deleting.")

            print("Sentinel file detected. Terminating script.")
            if self.error_flag:
                self.progress_queue.put("PROCESS CANCELLED: GelBandIDo cancelled due to unrecoverable errors.")
            else:
                self.progress_queue.put("PROCESS CANCELLED: GelBandIDo cancelled.")
            sys.exit()

    def get_dotnet_path(self, search_folders):
        for folder in search_folders:
            for root, dirnames, filenames in os.walk(folder):
                dirnames[:] = [d for d in dirnames if d not in ["Users", "$Recycle.Bin", "Windows", "ProgramData", "Spectronaut", "KNIME"]]
                dirnames[:] = [d for d in dirnames if not fnmatch.fnmatch(d, '*Spectronaut*')]
                dirnames[:] = [d for d in dirnames if not fnmatch.fnmatch(d, '*KNIME*')]
                for filename in filenames:
                    if filename == "dotnet.exe":
                        self.dotnet_path = os.path.join(root, filename)
                        print(self.dotnet_path)
                        return self.dotnet_path
            
    def concatenate_fasta_files(self):
        self.progress_queue.put("STARTING: Concatenating input and database fasta.")
        try:
            Path(self.master_fasta).parent.mkdir(parents=True, exist_ok=True)
            if os.path.exists(self.master_fasta):
                print("Merged fasta already exists--skipping\n")
            else:
                # Initialize a set to keep track of unique identifiers
                seen_identifiers = set()
                with Path(self.master_fasta).open('w') as output_file:
                    # First, process fasta_files to ensure they take priority
                    for fasta_file in self.fasta_files:
                        with Path(fasta_file).open() as input_file:
                            write_sequence = False
                            for line in input_file:
                                if line.startswith('>'):  # This is an identifier line
                                    identifier = line.strip()
                                    # Check if this identifier has already been seen
                                    if identifier not in seen_identifiers:
                                        seen_identifiers.add(identifier)
                                        write_sequence = True
                                    else:
                                        write_sequence = False
                                if write_sequence and line.strip():
                                    output_file.write(line)
                                    if not line.endswith('\n'):
                                        output_file.write('\n')
                    
                    # Then, process database_paths, skipping already seen identifiers
                    for fasta_file in self.database_paths:
                        with Path(fasta_file).open() as input_file:
                            write_sequence = False
                            for line in input_file:
                                if line.startswith('>'):  # This is an identifier line
                                    identifier = line.strip()
                                    # Check if this identifier has already been seen
                                    if identifier not in seen_identifiers:
                                        seen_identifiers.add(identifier)
                                        write_sequence = True
                                    else:
                                        write_sequence = False
                                if write_sequence and line.strip():
                                    output_file.write(line)
                                    if not line.endswith('\n'):
                                        output_file.write('\n')

            self.progress_queue.put("STEP COMPLETED: Files concatenated.")

        except Exception as e:
            self.progress_queue.put(f"ERROR: Error occurred while concatenating fasta files: {e}")
            self.error_flag = True



    def create_MaxQuant_par(self):
        self.progress_queue.put(
            f"STARTING: Creating template MaxQuant params file (v{self.MQ_version})."
        )
        try:
            # Create an empty params file for editing if one isn't provided
            if self.MQ_params:
                self.progress_queue.put("STEP COMPLETED: Using MaxQuant params file provided by user.")
            else:
                print("Creating an MQ par file!")
                new_MQ_params_name = f"mqpar_version_{self.MQ_version}.xml"
                if (Path(self.temp_op_folder) / new_MQ_params_name).exists():
                    print("Deleting existing default mqpar")
                    (Path(self.temp_op_folder) / new_MQ_params_name).unlink()
                
                self.MQ_params = Path(self.temp_op_folder) / new_MQ_params_name

                process_0 = subprocess.Popen(
                [self.MQ_path, '--create',  str(self.MQ_params)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
                result_stdout, result_stderr = process_0.communicate()
                print(result_stderr)
                if ".NET Core" in result_stderr:
                    print("MQ couldn't find dotnet for create folder. Retry with search for dotnet.")
                    self.progress_queue.put("UPDATE: MQ couldn't find dotnet.\nRetry with search for dotnet.")
                    process_0 = subprocess.Popen(
                    [self.get_dotnet_path(['D:\\Software', 'C:\\', self.AppData_path]),
                    self.MQ_path, '--create',  str(self.MQ_params)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)

                while process_0.poll() is None:
                    self.check_and_terminate_if_sentinel_exists()
                    time.sleep(2)

                result_stdout, result_stderr = process_0.communicate()

                if process_0.returncode != 0:
                    print("MaxQuant --create encountered an error.")
                    print("Error Output:")
                    print(result_stderr)
                    return  # Exit the method if the first subprocess fails

                print("MaxQuant --create ran successfully.")
                print("Standard Output:")
                print(result_stdout)

                self.progress_queue.put("STEP COMPLETED: Created template MaxQuant params file.")
                
        except Exception as e:
            self.progress_queue.put(
                f"ERROR: Error occured while creating template MaxQuant params file: {e}."
            )
            self.error_flag = True

    def edit_MQ_par(self):
        self.progress_queue.put(
            "STARTING: Editing MaxQuant params file."
        )
        try:
            print(Path(self.MQ_params))
            tree = etree.parse(Path(self.MQ_params))
            root = tree.getroot()

            # Editing fasta location
            fastaFile_block = root.find("./fastaFiles")

            for child in list(fastaFile_block)[1:]:
                fastaFile_block.remove(child)

            FastaFileInfo = fastaFile_block.find("FastaFileInfo")
            fasta_path = FastaFileInfo.find('fastaFilePath')
            fasta_path.text = os.path.basename(self.master_fasta)

            # Editing raw files and corresponding attributes
            branch_names = ["./filePaths", "./experiments", "./fractions", "./ptms", "./paramGroupIndices", "./referenceChannel"]
            branch_list = [root.find(branch_name) for branch_name in branch_names]

            for branch in branch_list:
                for child in list(branch)[1:]:
                    branch.remove(child)

            child_tags = ['string', 'string', 'short', 'boolean', 'int', 'string']
            child_list = [branch.find(tag) for tag, branch in zip(child_tags, branch_list)]

            ET_raw_files = list(self.conditions_dict.keys())
            experiments_list = [details['Experiment'] for details in self.conditions_dict.values()]
            common_length = len(self.conditions_dict)
            fractions = ['32767'] * common_length
            ptms = ['False'] * common_length
            pGIs = ['0'] * common_length
            refChannels = [''] * common_length

            par_text_lists = [ET_raw_files, experiments_list, fractions, ptms, pGIs, refChannels]

            for i, new_par_texts in enumerate(zip(*par_text_lists)):
                for par_child, new_par_text in zip(child_list, new_par_texts):
                    if i > 0:
                        new_dupe = copy.deepcopy(par_child)
                        new_dupe.text = new_par_text
                        branch_list[child_list.index(par_child)].append(new_dupe)
                    else:
                        par_child.text = new_par_text
            # Ensure AllPeptides table is produced!
            all_peps_branch = root.find("./writeAllPeptidesTable")
            all_peps_branch.text = "True"

            # Only if not using input MQ params
            if self.user_input_params == False:
                # Quantification type
                quant_mode_branch = root.find("./quantMode")
                quant_mode_dict = {'All': '0', 'Razor + Unique': '1', 'Unique': '2'}
                quant_mode_branch.text = quant_mode_dict[self.prot_quantification]

                # Edits to all parameter groups
                for param_group in root.findall("./parameterGroups/parameterGroup"):

                    # Max missed cleavages
                    missed_cleavage_branch = param_group.find("maxMissedCleavages")
                    if missed_cleavage_branch is not None:
                        missed_cleavage_branch.text = str(self.num_missed_cleavages)

                    # Fixed modifications
                    ## self.fixed_mods
                    fixed_mod_branches = param_group.findall("fixedModifications")
                    for fixed_mod_branch in fixed_mod_branches:
                        # Remove existing fixed modifications if needed
                        for child in list(fixed_mod_branch):
                            fixed_mod_branch.remove(child)
                        # Add new fixed modifications
                        for new_fixed_mod in self.fixed_mods:
                            new_fixed_mod_elem = etree.SubElement(fixed_mod_branch, "string")
                            new_fixed_mod_elem.text = new_fixed_mod

                    # Enzymes
                    ## self.enzymes(list)
                    enzymes_branches = param_group.findall("enzymes")
                    for enzymes_branch in enzymes_branches:
                        # Remove existing fixed modifications if needed
                        for child in list(enzymes_branch):
                            enzymes_branch.remove(child)
                        # Add new fixed modifications
                        for new_enzyme in self.enzymes:
                            new_enzyme_elem = etree.SubElement(enzymes_branch, "string")
                            new_enzyme_elem.text = new_enzyme

                    # Use enzymes first search
                    ## self.use_enzyme_first_search_str
                    use_enzyme_first_search_branch = param_group.find("useEnzymeFirstSearch")
                    if use_enzyme_first_search_branch is not None:
                        use_enzyme_first_search_branch.text = str(self.use_enzyme_first_search_str)

                    # First search enzymes 
                    ## self.fs_enzymes(list)
                    fs_enzymes_branches = param_group.findall("enzymesFirstSearch")
                    for fs_enzymes_branch in fs_enzymes_branches:
                        # Remove existing fixed modifications if needed
                        for child in list(fs_enzymes_branch):
                            fs_enzymes_branch.remove(child)
                        # Add new fixed modifications
                        for new_fs_enzyme in self.fs_enzymes:
                            new_fs_enzyme_elem = etree.SubElement(fs_enzymes_branch, "string")
                            new_fs_enzyme_elem.text = new_fs_enzyme

                    # Var modifications
                    ## self.var_mods
                    var_mod_branches = param_group.findall("variableModifications")
                    for var_mod_branch in var_mod_branches:
                        # Remove existing fixed modifications if needed
                        for child in list(var_mod_branch):
                            var_mod_branch.remove(child)
                        # Add new fixed modifications
                        for new_var_mod in self.var_mods:
                            new_var_mod_elem = etree.SubElement(var_mod_branch, "string")
                            new_var_mod_elem.text = new_var_mod


                
                # Editing parse rules
                identifierParseRule = FastaFileInfo.find("identifierParseRule")
                identifierParseRule.text = str(self.id_parse_rule)
                descriptionParseRule = FastaFileInfo.find("descriptionParseRule")
                descriptionParseRule.text = str(self.desc_parse_rule)

                #Editing fixed search folder for Andromeda
                fixedSearchFolder = root.find("./fixedSearchFolder")
                fixedSearchFolder.text = str(self.andromeda_path)

                # Editing option to try second peptide
                ## self.second_peptide_str
                secondPeptide = root.find("./secondPeptide")
                secondPeptide.text = str(self.second_peptide_str)

                # Editing option to match between runs
                ## self.match_between_runs
                matchBetweenRuns = root.find("./matchBetweenRuns")
                matchBetweenRuns.text = str(self.match_between_runs)

                ## self.num_threads
                numThreads = root.find("./numThreads")
                numThreads.text = str(self.num_threads)

            et = etree.ElementTree(root)
            #self.temp_MQ_params = Path(self.output_folder, os.path.basename(str(Path(self.MQ_params))).split('.xml')[0] + '_temp.xml')
            file_str = Path(self.MQ_params).name.replace('.xml','_temp.xml')
            self.progress_queue.put(f"TEMP: {file_str}")
            self.temp_MQ_params = Path(self.temp_op_folder) / file_str
            self.progress_queue.put(f"TEMP: {self.temp_MQ_params}")
            et.write(str(self.temp_MQ_params), pretty_print=True)
            self.progress_queue.put(
                "STEP COMPLETED: Edited MaxQuant params file."
            )

        except Exception as e:
            self.progress_queue.put(
                    f"ERROR: Error occured while editing MaxQuant params file: {e}"
                    )
            self.error_flag = True

    def run_MaxQuant(self):
        self.progress_queue.put(
            f"STARTING: Running MaxQuant version {self.MQ_version}."
        )
        file_name = Path(self.temp_MQ_params).name.replace('_temp.xml','_updated.xml')
        self.win_MQ_params_updated = Path(self.output_folder) / file_name
        print(self.win_MQ_params_updated)
        master_folder = str(Path(self.master_fasta).parent)
        print(master_folder)
        print(self.MQ_op_folder)

        if not self.MQ_op_folder.exists():

            #self.check_and_terminate_if_sentinel_exists()
            
            if self.win_MQ_params_updated.exists():
                self.win_MQ_params_updated.unlink()

            # Start changeFolder subprocess
            process_1 = subprocess.Popen(
                [
                self.MQ_path, str(self.temp_MQ_params), '--changeFolder', str(self.win_MQ_params_updated), str(master_folder), str(self.raw_folder)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
            result_stdout, result_stderr = process_1.communicate()
            if process_1.returncode != 0:
                if ".NET Core" in result_stderr:
                    print("MQ couldn't find dotnet. Retry with search for dotnet.")
                    self.progress_queue.put("UPDATE: MQ couldn't find dotnet.\nRetry with search for dotnet.")
                    self.dotnet_path = self.get_dotnet_path(['D:\\Software', 'C:\\', self.AppData_path])
                    self.progress_queue.put(self.dotnet_path)
                    process_1 = subprocess.Popen(
                    [self.dotnet_path,
                    self.MQ_path, str(self.temp_MQ_params), '--changeFolder', str(self.win_MQ_params_updated), str(master_folder), str(self.raw_folder)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
                    result_stdout, result_stderr = process_1.communicate()
        
            if process_1.returncode == 0:
                print("MaxQuant changeFolder ran successfully.")
                print("Standard Output:")
                print(result_stdout)
                self.progress_queue.put(
                    "STEP COMPLETED: Converted xml. Ready to begin MaxQuant."
                )
            else:
                print("MaxQuant changeFolder encountered an error.")
                print("Error Output:")
                print(result_stderr)
                self.progress_queue.put(
                    "ERROR: Error occured preparing for MaxQuant."
                )
                self.error_flag = True

            self.check_and_terminate_if_sentinel_exists()

            # If change folder ran with dotnet path, assume the same here                    

            # Start search subprocess
            process_test = subprocess.Popen(
                [
                self.MQ_path, '--dryrun', str(self.win_MQ_params_updated)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
            print(f"Started MaxQuant dryrun subprocess with PID: {process_test.pid}")
            self.progress_queue.put("UPDATE: Running dry run of MQ to assess dotnet.")                     
            result_stdout, result_stderr = process_test.communicate()

            if process_test.returncode != 0:
                if ".NET Core" in result_stderr:
                    print("MQ couldn't find dotnet. Retry with search for dotnet.")
                    self.progress_queue.put("UPDATE: MQ couldn't find dotnet.\nRetry with search for dotnet.")
                    self.dotnet_path = self.get_dotnet_path(['D:\\Software', 'C:\\', self.AppData_path])
                    self.progress_queue.put(self.dotnet_path)
                    command_list = [self.dotnet_path,self.MQ_path, str(self.win_MQ_params_updated)]
            else:
                command_list = [self.MQ_path, str(self.win_MQ_params_updated)]
            
            process_2 = subprocess.Popen(
                command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True
                )
            print(f"Started MaxQuant subprocess with PID: {process_2.pid}")

            # Periodically check if the sentinel file exists while the subprocess is running
            while True:
                if process_2.poll() is not None:
                    print("Process 2 has ended.")
                    break
                self.check_stop_queue()
                if self.stop_requested:
                    print("Stop request detected during Process 2.")
                    self.terminate_external_processes(process_2)
                    break
                time.sleep(2)

            result_stdout, result_stderr = process_2.communicate()
            print("Communicate command executed after Process 2.")

            if process_2.returncode == 0:
                print("MaxQuant Search ran successfully.")
                print("Standard Output:")
                print(result_stdout)
                self.progress_queue.put(
                    f"STEP COMPLETED: Ran MaxQuant version {self.MQ_version}."
                )
            else:
                print("MaxQuant Search encountered an error.")
                print("Error Output:")
                print(result_stderr)
                print(len(str(result_stderr)))
                if len(str(result_stderr)) > 0:
                    self.progress_queue.put(
                        f"ERROR: MaxQuant version {self.MQ_version} completed with error."
                    )
                    self.cleanup_directory(self.MQ_op_folder)
                    self.error_flag = True
                else:
                    self.cleanup_directory(self.MQ_op_folder)
            
        else:
            self.progress_queue.put(
                "STEP COMPLETED: Already found MaxQuant result in output location--continuing without running."
            )
            print("MaxQuant output folder already available.")

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
            print("HELLLLLOOOOOO")
        except Exception as e:
            self.progress_queue.put(f"ERROR: Error in process: {e}")
            self.error_flag = True
            self.check_and_terminate_if_sentinel_exists()
                        
# Operations for converting string arguments to appropriate type
def comma_separated_string_to_list(comma_separated_string):
    if comma_separated_string == "":
        return None
    else:
        return comma_separated_string.split(',')

def str_to_bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
    
def str_to_int(v):
    if isinstance(v, int):
        return v
    else:
        try:
            return(int(v))
        except ValueError:
            raise argparse.ArgumentTypeError('Numerical value expected.')
    
def str_to_float(v):
    if isinstance(v, float):
        return v
    else:
        try:
            return(float(v))
        except ValueError:
            raise argparse.ArgumentTypeError('Numerical value expected.')