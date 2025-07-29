# app/automations/conversion/state.py
job_logs = {}  # in-memory store: job_id → logs (list)

def log(job_id, msg):
    print(msg)
    if job_id in job_logs:
        job_logs[job_id].append(msg)
