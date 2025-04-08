# tests/test_maxquant_submission.py

import os
import pytest
from flask import Flask
from src.core.routes import app  # your Flask app that has /maxquant route
from src.database.jobs_db import JobsDB

@pytest.fixture
def client():
    # This sets up your Flask test client
    with app.test_client() as client:
        yield client

def test_maxquant_job_submission(client):
    # We'll simulate an HTTP POST to /maxquant, passing form data
    response = client.post('/maxquant', data={
        'fasta_folder': '/path/to/fasta',
        'output_folder': '/path/to/output',
        'conditions_file': '/path/to/conditions.txt',
        'mq_path': '/path/to/MaxQuantCmd.exe',
        'mq_version': '2.4.0.0',
        'database_choices': 'human',  # or request.form.getlist scenario
        'job_name': 'Test_MaxQuant_Job'
    })

    # Check response
    assert response.status_code == 200
    resp_text = response.get_data(as_text=True)
    assert "A watcher (ID:" in resp_text  # or similar text that indicates success

    # Optionally check the DB
    jobs_db = JobsDB(db_path="config/jobs.db")
    # fetch recently added job by name, e.g.:
    job = jobs_db.get_job_by_name("Test_MaxQuant_Job")
    assert job is not None
    assert job['job_type'] == 'maxquant'

    # If you want to check that it used simulation:
    #  - you can add a param & route condition, or read logs, etc.
