import ray
import math
import random


@ray.remote
class ProgressActor:
    def __init__(self, total_num_samples: int):
        self.total_num_samples = total_num_samples
        self.num_samples_completed_per_task = {}

    def report_progress(self, task_id: int, num_samples_completed: int) -> None:
        self.num_samples_completed_per_task[task_id] = num_samples_completed

    def get_progress(self) -> float:
        return (
            sum(self.num_samples_completed_per_task.values()) / self.total_num_samples
        )

    def raise_error_for_testing(self) -> None:
        raise Exception("Made-up error for Task 3. To fix, just delete this line.")


@ray.remote
def sampling_task(
    num_samples: int, task_id: int, progress_actor: ray.actor.ActorHandle
) -> int:
    num_inside = 0
    for i in range(num_samples):
        x, y = random.uniform(-1, 1), random.uniform(-1, 1)
        if math.hypot(x, y) <= 1:
            num_inside += 1

        # Report progress every 1 million samples.
        if (i + 1) % 1_000_000 == 0:
            # This is async.
            progress_actor.report_progress.remote(task_id, i + 1)

        if (i + 1) % (num_samples // 4) == 0:
            print("WARNING: Made-up warning. If you're on Task 2, trigger an alert.")

    # Report the final progress.
    progress_actor.report_progress.remote(task_id, num_samples)
    return num_inside
