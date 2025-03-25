import os
import threading
import webview
from flask import Flask, render_template, request, redirect, url_for
from components.searches.run_maxquant import MaxQuant_handler

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/maxquant', methods=['GET', 'POST'])
def maxquant():
    if request.method == 'POST':
        try:
            # Extract form data
            fasta_folder = request.files.getlist('fasta_folder')
            output_folder = request.files.getlist('output_folder')
            conditions_file = request.files['conditions_file']
            mq_path = request.files['mq_path']
            mq_version = request.form.get('mq_version')
            dbs = request.form.getlist('database_choices')
            job_name = request.form.get('job_name') or "MaxQuantJob"

            # Save uploaded files locally (simplified)
            local_output = os.path.join(app.config['UPLOAD_FOLDER'], job_name)
            os.makedirs(local_output, exist_ok=True)
            cond_path = os.path.join(local_output, conditions_file.filename)
            mq_path.save(os.path.join(local_output, mq_path.filename))
            conditions_file.save(cond_path)

            # Placeholder for actual run logic
            thread = threading.Thread(target=launch_maxquant_job, args=(mq_version, mq_path.filename, cond_path, dbs, local_output))
            thread.start()

            return f"Job {job_name} submitted successfully."
        except Exception as e:
            return f"Error submitting MaxQuant job: {str(e)}"
    return render_template('maxquant.html')

def launch_maxquant_job(mq_version, mq_path, conditions_file, dbs, output_folder):
    print("Pretending to launch MaxQuant...")
    # Here you'd create MaxQuant_handler(...).run_MaxQuant_cli()
    # For now it's stubbed
    pass

@app.route('/diann')
def diann():
    return render_template('diann.html')

@app.route('/spectronaut')
def spectronaut():
    return render_template('spectronaut.html')

@app.route('/quantms')
def quantms():
    return render_template('quantms.html')

@app.route('/gelbandido')
def gelbandido():
    return render_template('gelbandido.html')

@app.route('/dianalyzer')
def dianalyzer():
    return render_template('dianalyzer.html')

@app.route('/job-monitor')
def job_monitor():
    return render_template('job_monitor.html')

@app.route('/config')
def config():
    return render_template('config.html')

def start_flask():
    app.run(debug=False, port=5000)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=start_flask)
    flask_thread.daemon = True
    flask_thread.start()
    webview.create_window("Prot Core UI", "http://127.0.0.1:5000", min_size=(800,600))
    webview.start()