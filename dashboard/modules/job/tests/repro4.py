import ray

print(ray.init())

import time

print("sleeping")
while True:
    time.sleep(5)