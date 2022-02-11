from dataclasses import dataclass, replace
from enum import Enum
import time
from typing import Any, Dict, Optional, Tuple, Union
import pickle
import logging

from ray import ray_constants
from ray.experimental.internal_kv import (
    _internal_kv_initialized,
    _internal_kv_get,
    _internal_kv_list,
    _internal_kv_put,
)
from ray._private.runtime_env.packaging import parse_uri

# NOTE(edoakes): these constants should be considered a public API because
# they're exposed in the snapshot API.
JOB_ID_METADATA_KEY = "job_submission_id"
JOB_NAME_METADATA_KEY = "job_name"

# Version 0 -> 1: Added log streaming and changed behavior of job logs cli.
CURRENT_VERSION = "1"

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

    def __str__(self):
        return f"{self.value}"

    def is_terminal(self):
        return self.value in {"STOPPED", "SUCCEEDED", "FAILED"}


@dataclass
class JobStatusInfo:
    status: JobStatus
    message: Optional[str] = None

    def __post_init__(self):
        if self.message is None:
            if self.status == JobStatus.PENDING:
                self.message = (
                    "Job has not started yet, likely waiting "
                    "for the runtime_env to be set up."
                )
            elif self.status == JobStatus.RUNNING:
                self.message = "Job is currently running."
            elif self.status == JobStatus.STOPPED:
                self.message = "Job was intentionally stopped."
            elif self.status == JobStatus.SUCCEEDED:
                self.message = "Job finished successfully."
            elif self.status == JobStatus.FAILED:
                self.message = "Job failed."


@dataclass
class JobData:
    status_info: JobStatusInfo
    start_time: int
    end_time: int = None
    metadata: Optional[Dict[str, str]] = None
    runtime_env: Optional[Dict[str, Any]] = None
    namespace: Optional[str] = None


class JobDataStorageClient:
    """
    Interface to put and get job data from the Internal KV store.
    """

    JOB_DATA_KEY_PREFIX = "_ray_internal_job_data_"
    JOB_DATA_KEY = f"{JOB_DATA_KEY_PREFIX}{{job_id}}"

    def __init__(self):
        assert _internal_kv_initialized()

    def put_data(self, job_id: str, data: JobData):
        _internal_kv_put(
            self.JOB_DATA_KEY.format(job_id=job_id),
            pickle.dumps(data),
            namespace=ray_constants.KV_NAMESPACE_JOB,
        )

    def get_data(self, job_id: str) -> Optional[JobData]:
        logger.error("CALLED GET DATA " + str(job_id))
        logger.error(
            "KV LIST FROM GET DATA: "
            + str(_internal_kv_list("", namespace=ray_constants.KV_NAMESPACE_JOB))
        )
        # raise ValueError
        pickled_data = _internal_kv_get(
            self.JOB_DATA_KEY.format(job_id=job_id),
            namespace=ray_constants.KV_NAMESPACE_JOB,
        )
        logger.error("PICKLED DATA: " + str(pickled_data))
        if pickled_data is None:
            return None
        else:
            return pickle.loads(pickled_data)

    def put_status(self, job_id: str, status: Union[JobStatus, JobStatusInfo]):
        """Puts or updates job status.  Sets end_time if status is terminal."""

        if isinstance(status, JobStatus):
            status_info = JobStatusInfo(status=status)
        elif isinstance(status, JobStatusInfo):
            status_info = status
        else:
            assert False, "status must be JobStatus or JobStatusInfo."

        old_data = self.get_data(job_id)

        if old_data is not None:
            if (
                status_info.status != old_data.status_info.status
                and old_data.status_info.status.is_terminal()
            ):
                assert False, "Attempted to change job status from a terminal state."
            new_data = replace(old_data, status_info=status_info)
        else:
            new_data = JobData(status_info=status_info)

        if status_info.status.is_terminal():
            new_data.end_time = int(time.time())

        self.put_data(job_id, new_data)

    def get_status(self, job_id: str) -> Optional[JobStatusInfo]:
        job_data = self.get_data(job_id)
        if job_data is None:
            return None
        else:
            return job_data.status_info

    def get_all_jobs(self) -> Dict[str, JobData]:
        raw_job_ids_with_prefixes = _internal_kv_list(
            self.JOB_DATA_KEY_PREFIX, namespace=ray_constants.KV_NAMESPACE_JOB
        )
        print("ALL JOB IDS:  ", raw_job_ids_with_prefixes)
        logger.error("ALL JOB IDS: " + str(raw_job_ids_with_prefixes))
        logger.error(
            "ALL INTERNAL KV: "
            + str(_internal_kv_list("", namespace=ray_constants.KV_NAMESPACE_JOB))
        )
        job_ids_with_prefixes = [
            job_id.decode() for job_id in raw_job_ids_with_prefixes
        ]
        job_ids = []
        for job_id_with_prefix in job_ids_with_prefixes:
            assert job_id_with_prefix.startswith(
                self.JOB_DATA_KEY_PREFIX
            ), "Unexpected format for internal_kv key for Job submission"
            job_ids.append(job_id_with_prefix[len(self.JOB_DATA_KEY_PREFIX) :])
        return {job_id: self.get_data(job_id) for job_id in job_ids}


def uri_to_http_components(package_uri: str) -> Tuple[str, str]:
    if not package_uri.endswith(".zip"):
        raise ValueError(f"package_uri ({package_uri}) does not end in .zip")
    # We need to strip the gcs:// prefix and .zip suffix to make it
    # possible to pass the package_uri over HTTP.
    protocol, package_name = parse_uri(package_uri)
    return protocol.value, package_name[: -len(".zip")]


def http_uri_components_to_uri(protocol: str, package_name: str) -> str:
    if package_name.endswith(".zip"):
        raise ValueError(f"package_name ({package_name}) should not end in .zip")
    return f"{protocol}://{package_name}.zip"


def validate_request_type(json_data: Dict[str, Any], request_type: dataclass) -> Any:
    return request_type(**json_data)


@dataclass
class VersionResponse:
    version: str
    ray_version: str
    ray_commit: str


@dataclass
class JobSubmitRequest:
    # Command to start execution, ex: "python script.py"
    entrypoint: str
    # Optional job_id to specify for the job. If the job_id is not specified,
    # one will be generated. If a job with the same job_id already exists, it
    # will be rejected.
    job_id: Optional[str] = None
    # Dict to setup execution environment.
    runtime_env: Optional[Dict[str, Any]] = None
    # Metadata to pass in to the JobConfig.
    metadata: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if not isinstance(self.entrypoint, str):
            raise TypeError(f"entrypoint must be a string, got {type(self.entrypoint)}")

        if self.job_id is not None and not isinstance(self.job_id, str):
            raise TypeError(
                f"job_id must be a string if provided, got {type(self.job_id)}"
            )

        if self.runtime_env is not None:
            if not isinstance(self.runtime_env, dict):
                raise TypeError(
                    f"runtime_env must be a dict, got {type(self.runtime_env)}"
                )
            else:
                for k in self.runtime_env.keys():
                    if not isinstance(k, str):
                        raise TypeError(
                            f"runtime_env keys must be strings, got {type(k)}"
                        )

        if self.metadata is not None:
            if not isinstance(self.metadata, dict):
                raise TypeError(f"metadata must be a dict, got {type(self.metadata)}")
            else:
                for k in self.metadata.keys():
                    if not isinstance(k, str):
                        raise TypeError(f"metadata keys must be strings, got {type(k)}")
                for v in self.metadata.values():
                    if not isinstance(v, str):
                        raise TypeError(
                            f"metadata values must be strings, got {type(v)}"
                        )


@dataclass
class JobSubmitResponse:
    job_id: str


@dataclass
class JobStopResponse:
    stopped: bool


@dataclass
class JobStatusResponse:
    status: JobStatus
    message: Optional[str]


# TODO(jiaodong): Support log streaming #19415
@dataclass
class JobLogsResponse:
    logs: str
