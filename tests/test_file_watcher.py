# tests/test_file_watcher.py
import os
from pathlib import Path
import keyboard
from src.database.watcher_db import WatcherDB
from src.watchers.watcher_manager import WatcherManager
from src.core.job_queue_manager import JobQueueManager

project_root = Path(os.path.dirname(os.path.abspath(__file__))).parent
db = WatcherDB("config/watchers.db")
job_queue_manager = JobQueueManager()
manager = WatcherManager(db, job_queue_manager)

db.delete_test_watchers(prefix="test_")

test_folder = project_root / "test_data" / "input"
test_folder.mkdir(parents=True, exist_ok=True)
print(f"Ensured test directory exists at: {test_folder}")

watcher_id = db.add_watcher(
    folder_path=str(test_folder),
    file_pattern="1.txt;2.txt;3.txt",
    job_type="diann_handler",
    job_demands="cpu:1",
    job_name_prefix="test_diann_job"
)
print(f"Added test watcher with ID: {watcher_id}")

print("Loading and starting watchers...")
manager.load_watchers()
print("Loaded watchers:", [(w.watcher_id, w.config["job_name_prefix"]) for w in manager.watchers.values()])
manager.start_all()

print("Watchers are running. Press 's' to stop and exit the test...")
while not keyboard.is_pressed('s'):
    pass

print("Stopping all watchers...")
manager.stop_all()
db.close()
print("Test completed.")