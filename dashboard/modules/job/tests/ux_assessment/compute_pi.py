import ray
import time
import os
import emoji
from compute_pi_utils import ProgressActor, sampling_task

# Change this to match your cluster scale.
NUM_SAMPLING_TASKS = os.getenv("NUM_SAMPLING_TASKS", 2)
NUM_SAMPLES_PER_TASK = os.getenv("NUM_SAMPLES_PER_TASK", 100_000)
TOTAL_NUM_SAMPLES = NUM_SAMPLING_TASKS * NUM_SAMPLES_PER_TASK

print("Using {} sampling tasks".format(NUM_SAMPLING_TASKS))
print("Using {} samples per task".format(NUM_SAMPLES_PER_TASK))
print("Total number of samples: {}".format(TOTAL_NUM_SAMPLES))

ray.init()

# Create the progress actor.
progress_actor = ProgressActor.remote(TOTAL_NUM_SAMPLES)

# Create and execute all sampling tasks in parallel.
results = [
    sampling_task.remote(NUM_SAMPLES_PER_TASK, i, progress_actor)
    for i in range(NUM_SAMPLING_TASKS)
]

# Query progress periodically.
while True:
    progress = ray.get(progress_actor.get_progress.remote())
    print(f"Progress: {int(progress * 100)}%")

    if progress == 1:
        break

    time.sleep(1)

# Get all the sampling tasks results.
total_num_inside = sum(ray.get(results))
pi = (total_num_inside * 4) / TOTAL_NUM_SAMPLES
print(f"Estimated value of π is: {pi}")
result = emoji.emojize('Ray is good :thumbs_up:')
print(result)