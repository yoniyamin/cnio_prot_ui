from datetime import datetime
from pathlib import Path
import pandas as pd

class SDRFWriter:
    def __init__(self, conditions_file, op_file, read_dir):
        self.conditions_file = Path(conditions_file)
        self.op_file = op_file
        self.read_dir = read_dir

    def write_sdrf(self):
        """Generate the SDRF file based on the conditions file."""
        # Load conditions file
        df_conditions = pd.read_csv(self.conditions_file, sep="\t") if self.conditions_file.suffix == ".tsv" else pd.read_excel(self.conditions_file)

        # Prepare SDRF rows
        sdrf_rows = []
        for _, row in df_conditions.iterrows():
            sample_name = row['Sample.Name']
            bio_rep = row['Bio.Rep']
            tech_rep = row['Tech.Rep']
            condition = row['Condition']
            read_uri = (Path(self.read_dir) / f"{sample_name}.raw").as_posix()

            sdrf_rows.append([
                sample_name,  # source name
                bio_rep,  # characteristics[biological replicate]
                "homo sapiens",  # characteristics[organism]
                "unknown",  # characteristics[organism part]
                "unknown",  # characteristics[disease]
                "unknown",  # characteristics[cell type]
                f"{sample_name}_Rep{bio_rep}",  # assay name
                f"{sample_name}.raw",  # comment[data file]
                f"{read_uri}",  # comment[file uri]
                "AC=MS:1002038;NT=label free sample",  # comment[label]
                "NT=Oxidation;MT=Variable;TA=M;AC=Unimod:35",  # comment[modification parameters]
                "NT=Carbamidomethyl;TA=C;MT=fixed;AC=UNIMOD:4",  # comment[modification parameters]
                "NT=Trypsin",  # comment[cleavage agent details]
                "20 ppm",  # comment[precursor mass tolerance]
                "0.05 Da",  # comment[fragment mass tolerance]
                "NT=Data-Independent Acquisition;AC=NCIT:C161786",  # comment[proteomics data acquisition method]
                "NT=Orbitrap Astral",  # comment[instrument]
                tech_rep,  # comment[technical replicate]
                tech_rep,  # comment[fraction identifier]
                condition,  # factor value[condition]
                condition.split("_")[0]  # factor value[disease]
            ])

        # SDRF columns
        sdrf_columns = [
            "source name", "characteristics[biological replicate]", "characteristics[organism]",
            "characteristics[organism part]", "characteristics[disease]", "characteristics[cell type]",
            "assay name", "comment[data file]", "comment[file uri]", "comment[label]",
            "comment[modification parameters]", "comment[modification parameters]",
            "comment[cleavage agent details]", "comment[precursor mass tolerance]",
            "comment[fragment mass tolerance]", "comment[proteomics data acquisition method]",
            "comment[instrument]", "comment[technical replicate]", "comment[fraction identifier]",
            "factor value[condition]", "factor value[disease]"
        ]

        # Save SDRF to file
        df_sdrf = pd.DataFrame(sdrf_rows, columns=sdrf_columns)
        print(df_sdrf)
        df_sdrf.to_csv(str(self.op_file), sep="\t", index=False)

class SBatchScriptWriter:
    def __init__(self, sdrf_file, save_path, job_id=None, ntasks=1, cpus_per_task=8, mem="300G", time="3:0:0", user=None, module="singularity", nextflow_command="nextflow run main.nf -c nextflow_parallel_PCU.config -profile singularity"):
        self.save_path = Path(save_path)
        self.sdrf_file = Path(sdrf_file)
        self.job_id = job_id
        self.ntasks = ntasks
        self.cpus_per_task = cpus_per_task
        self.mem = mem
        self.time = time
        self.user = user
        self.module = module
        self.nextflow_command = nextflow_command

    def write_script(self):
        # TODO: Include database check/copy
        """Writes the sbatch script to the specified path with Linux newlines."""
        if not self.job_id:
            self.job_id = f'{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        
        script_content = f"""#!/bin/sh
#SBATCH --job-name={self.job_id}
#SBATCH --ntasks={self.ntasks}
#SBATCH --cpus-per-task={self.cpus_per_task}
#SBATCH --mem={self.mem}
#SBATCH --time={self.time}"""
        if self.user:
            script_content += f"""
#SBATCH --mail-user={self.user}@cnio.es
#SBATCH --mail-type=END,FAIL
"""

        script_content += f"""
module load {self.module}
NXF_LOG_FILE={self.job_id}.log {self.nextflow_command} --input {Path(self.sdrf_file).name} --database Human__OPSPG_20614_w_525_CONTAMINANTS_UNIPROT_FORMAT.fasta --outdir {self.job_id}
"""
        # Write the script with Linux newlines
        with self.save_path.open(mode="w", newline="\n") as file:
            file.write(script_content)

# Example usage
if __name__ == "__main__":
    sdfr_writer = SDRFWriter(
        op_file=r"C:\Users\lwoods\Documents\LW_Projects_folder\HPC\demo_conditions.sdrf",
        conditions_file=r"C:\Users\lwoods\Documents\LW_Projects_folder\HPC\demo_conditions.xlsx",
        read_dir="/storage/scratch01/groups/pcu/pancaid_2025/quantms/"
    )
    sdfr_writer.write_sdrf()
    
    script_writer = SBatchScriptWriter(
        sdrf_file=r"C:\Users\lwoods\Documents\LW_Projects_folder\HPC\demo_conditions.sdrf",
        save_path=r"C:\Users\lwoods\Documents\LW_Projects_folder\HPC\test_dia_sbatch2.sh",
        user="lwoods"
    )
    script_writer.write_script()