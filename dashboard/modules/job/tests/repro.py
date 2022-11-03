import ray

from ray.job_submission import JobSubmissionClient, JobStatus

ctx = ray.init()
gcs_address = ctx["gcs_address"]
# client = JobSubmissionClient()

# Submit a job.
# # job_id = client.submit_job(entrypoint="echo hi")

# import time

# time.sleep(5)

# assert client.get_job_status(job_id) == JobStatus.SUCCEEDED
# import time
# time.sleep(5)
import subprocess

subprocess.run(["python", "repro2.py", "--address", gcs_address])
import time
time.sleep(5)

