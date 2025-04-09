import os
import time
import pytest
from app_core import app, watcher_db, jobs_db, job_queue_manager
from src.core.file_utils import ensure_directory_exists

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

@pytest.fixture
def test_data_dir(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    conditions_file = input_dir / "conditions.tsv"
    with open(conditions_file, 'w') as f:
        f.write("Raw file\tExperiment\nfile1.raw\tExp1\nfile2.raw\tExp1\n")
    return input_dir

def test_maxquant_submission_simulated(client, test_data_dir):
    # Prepare test data
    fasta_folder = str(test_data_dir)
    output_folder = str(test_data_dir / "output")
    conditions_file = str(test_data_dir / "conditions.tsv")
    mq_path = "C:/fake/path/MaxQuantCmd.exe"  # Simulated path
    ensure_directory_exists(output_folder)

    # Submit simulated MaxQuant job
    response = client.post(
        '/maxquant?simulate=true',
        data={
            'fasta_folder': fasta_folder,
            'output_folder': output_folder,
            'conditions_file': conditions_file,
            'mq_path': mq_path,
            'mq_version': '2.1.4.0',
            'database_choices': ['HUMAN'],
            'job_name': 'TestSimMaxQuant'
        }
    )

    assert response.status_code == 200
    assert b"Job Submitted Successfully" in response.data

    # Check watcher creation
    watchers = watcher_db.get_watchers()
    assert len(watchers) > 0
    watcher_id = watchers[0][0]

    # Check job creation
    jobs = jobs_db.get_jobs_by_watcher_id(watcher_id)
    assert len(jobs) == 1
    assert jobs[0]['job_name'] == 'TestSimMaxQuant'
    assert jobs[0]['status'] in ['waiting', 'running', 'completed']

    # Wait for simulation to complete (adjust timing as needed)
    time.sleep(10)  # Simulation takes ~10 seconds
    job_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'TestSimMaxQuant')
    with open(os.path.join(job_dir, "status.txt")) as f:
        status = f.read()
    assert status == "complete"

def test_maxquant_submission_stop(client, test_data_dir):
    # Submit simulated job
    response = client.post(
        '/maxquant?simulate=true',
        data={
            'fasta_folder': str(test_data_dir),
            'output_folder': str(test_data_dir / "output"),
            'conditions_file': str(test_data_dir / "conditions.tsv"),
            'mq_path': "C:/fake/path/MaxQuantCmd.exe",
            'mq_version': '2.1.4.0',
            'database_choices': ['HUMAN'],
            'job_name': 'TestStopMaxQuant'
        }
    )

    assert response.status_code == 200

    # Get job ID
    jobs = jobs_db.get_all_jobs()
    job_id = next(job['job_id'] for job in jobs if job['job_name'] == 'TestStopMaxQuant')

    # Stop the job
    response = client.post(f'/api/jobs/{job_id}/stop')
    assert response.status_code == 200
    assert b"Job TestStopMaxQuant cancelled" in response.data

    # Verify status
    updated_job = jobs_db.get_job(job_id)
    assert updated_job['status'] == 'cancelled'