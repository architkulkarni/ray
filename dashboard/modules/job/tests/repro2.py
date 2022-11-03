import ray
import repro3

import logging
import sys
import argparse

logger = logging.getLogger(__name__)
repro3.do_info_log()

# Get argument --gcs-address from command line.
parser = argparse.ArgumentParser()
parser.add_argument("--address", required=True)
args = parser.parse_args()
gcs_address = args.address

ray.init(address=gcs_address, log_to_driver=False)

logger.error("Logging after init")
repro3.do_info_log()
import time

time.sleep(5)
repro3.do_info_log()
logger.error("Logging after sleep")

ray.shutdown()