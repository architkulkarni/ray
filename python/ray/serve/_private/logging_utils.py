import json
import copy
import logging
import os
from typing import Optional

import ray
from ray.serve._private.common import ServeComponentType
from ray.serve._private.constants import (
    DEBUG_LOG_ENV_VAR,
    SERVE_LOGGER_NAME,
    RAY_SERVE_ENABLE_JSON_LOGGING,
    SERVE_LOG_RECORD_FORMAT,
    SERVE_LOG_REQUEST_ID,
    SERVE_LOG_ROUTE,
    SERVE_LOG_APPLICATION,
    SERVE_LOG_MESSAGE,
    SERVE_LOG_DEPLOYMENT,
    SERVE_LOG_COMPONENT,
    SERVE_LOG_COMPONENT_ID,
    SERVE_LOG_TIME,
    SERVE_LOG_LEVEL_NAME,
    SERVE_LOG_REPLICA,
)


LOG_FILE_FMT = "{component_name}_{component_id}.log"


class ServeJSONFormatter(logging.Formatter):
    """Serve Logging Json Formatter

    The formatter will generate the json log format on the fly
    based on the field of record.
    """

    def __init__(
        self,
        component_name: str,
        component_id: str,
        component_type: Optional[ServeComponentType] = None,
    ):
        self.component_log_fmt = {
            SERVE_LOG_LEVEL_NAME: SERVE_LOG_RECORD_FORMAT[SERVE_LOG_LEVEL_NAME],
            SERVE_LOG_TIME: SERVE_LOG_RECORD_FORMAT[SERVE_LOG_TIME],
        }
        if component_type and component_type == ServeComponentType.DEPLOYMENT:
            self.component_log_fmt[SERVE_LOG_DEPLOYMENT] = component_name
            self.component_log_fmt[SERVE_LOG_REPLICA] = component_id
        else:
            self.component_log_fmt[SERVE_LOG_COMPONENT] = component_name
            self.component_log_fmt[SERVE_LOG_COMPONENT_ID] = component_id

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record into json format.

        Args:
            record: The log record to be formatted.

            Returns:
                The formatted log record in json format.
        """
        record_format = copy.deepcopy(self.component_log_fmt)
        if SERVE_LOG_REQUEST_ID in record.__dict__:
            record_format[SERVE_LOG_REQUEST_ID] = SERVE_LOG_RECORD_FORMAT[
                SERVE_LOG_REQUEST_ID
            ]
        if SERVE_LOG_ROUTE in record.__dict__:
            record_format[SERVE_LOG_ROUTE] = SERVE_LOG_RECORD_FORMAT[SERVE_LOG_ROUTE]
        if SERVE_LOG_APPLICATION in record.__dict__:
            record_format[SERVE_LOG_APPLICATION] = SERVE_LOG_RECORD_FORMAT[
                SERVE_LOG_APPLICATION
            ]

        record_format[SERVE_LOG_MESSAGE] = SERVE_LOG_RECORD_FORMAT[SERVE_LOG_MESSAGE]

        # create a formatter using the format string
        formatter = logging.Formatter(json.dumps(record_format))

        # format the log record using the formatter
        return formatter.format(record)


class ServeFormatter(logging.Formatter):
    """Serve Logging Formatter

    The formatter will generate the log format on the fly based on the field of record.
    """

    COMPONENT_LOG_FMT = f"%({SERVE_LOG_LEVEL_NAME})s %({SERVE_LOG_TIME})s {{{SERVE_LOG_COMPONENT}}} {{{SERVE_LOG_COMPONENT_ID}}} "  # noqa:E501

    def __init__(
        self,
        component_name: str,
        component_id: str,
    ):
        self.component_log_fmt = ServeFormatter.COMPONENT_LOG_FMT.format(
            component_name=component_name, component_id=component_id
        )

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record into the format string.

        Args:
            record: The log record to be formatted.

            Returns:
                The formatted log record in string format.
        """
        record_format = self.component_log_fmt
        record_formats_attrs = []
        if SERVE_LOG_REQUEST_ID in record.__dict__:
            record_formats_attrs.append(SERVE_LOG_RECORD_FORMAT[SERVE_LOG_REQUEST_ID])
        if SERVE_LOG_ROUTE in record.__dict__:
            record_formats_attrs.append(SERVE_LOG_RECORD_FORMAT[SERVE_LOG_ROUTE])
        if SERVE_LOG_APPLICATION in record.__dict__:
            record_formats_attrs.append(SERVE_LOG_RECORD_FORMAT[SERVE_LOG_APPLICATION])
        record_formats_attrs.append(SERVE_LOG_RECORD_FORMAT[SERVE_LOG_MESSAGE])
        record_format += " ".join(record_formats_attrs)

        # create a formatter using the format string
        formatter = logging.Formatter(record_format)

        # format the log record using the formatter
        return formatter.format(record)


def access_log_msg(*, method: str, status: str, latency_ms: float):
    """Returns a formatted message for an HTTP or ServeHandle access log."""
    return f"{method.upper()} {status.upper()} {latency_ms:.1f}ms"


def log_to_stderr_filter(record: logging.LogRecord) -> bool:
    """Filters log records based on a parameter in the `extra` dictionary."""
    if not hasattr(record, "log_to_stderr") or record.log_to_stderr is None:
        return True

    return record.log_to_stderr


def get_component_logger_file_path() -> Optional[str]:
    """Returns the relative file path for the Serve logger, if it exists.

    If a logger was configured through configure_component_logger() for the Serve
    component that's calling this function, this returns the location of the log file
    relative to the ray logs directory.
    """
    logger = logging.getLogger(SERVE_LOGGER_NAME)
    for handler in logger.handlers:
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            absolute_path = handler.baseFilename
            ray_logs_dir = ray._private.worker._global_node.get_logs_dir_path()
            if absolute_path.startswith(ray_logs_dir):
                return absolute_path[len(ray_logs_dir) :]


def configure_component_logger(
    *,
    component_name: str,
    component_id: str,
    component_type: Optional[ServeComponentType] = None,
    log_level: int = logging.INFO,
    max_bytes: Optional[int] = None,
    backup_count: Optional[int] = None,
):
    """Returns a logger to be used by a Serve component.

    The logger will log using a standard format to make components identifiable
    using the provided name and unique ID for this instance (e.g., replica ID).

    This logger will *not* propagate its log messages to the parent logger(s).
    """
    logger = logging.getLogger(SERVE_LOGGER_NAME)
    logger.propagate = False
    logger.setLevel(log_level)
    if os.environ.get(DEBUG_LOG_ENV_VAR, "0") != "0":
        logger.setLevel(logging.DEBUG)

    factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        request_context = ray.serve.context._serve_request_context.get()
        record = factory(*args, **kwargs)
        if request_context.route:
            setattr(record, SERVE_LOG_ROUTE, request_context.route)
        if request_context.request_id:
            setattr(record, SERVE_LOG_REQUEST_ID, request_context.request_id)
        if request_context.app_name:
            setattr(record, SERVE_LOG_APPLICATION, request_context.app_name)
        return record

    logging.setLogRecordFactory(record_factory)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(ServeFormatter(component_name, component_id))
    stream_handler.addFilter(log_to_stderr_filter)
    logger.addHandler(stream_handler)

    logs_dir = os.path.join(
        ray._private.worker._global_node.get_logs_dir_path(), "serve"
    )
    os.makedirs(logs_dir, exist_ok=True)
    if max_bytes is None:
        max_bytes = ray._private.worker._global_node.max_bytes
    if backup_count is None:
        backup_count = ray._private.worker._global_node.backup_count

    # For DEPLOYMENT component type, we want to log the deployment name
    # instead of adding the component type to the component name.
    component_log_file_name = component_name
    if component_type is not None:
        component_log_file_name = f"{component_type}_{component_name}"
        if component_type != ServeComponentType.DEPLOYMENT:
            component_name = f"{component_type}_{component_name}"
    log_file_name = LOG_FILE_FMT.format(
        component_name=component_log_file_name, component_id=component_id
    )
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(logs_dir, log_file_name),
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    if RAY_SERVE_ENABLE_JSON_LOGGING:
        file_handler.setFormatter(
            ServeJSONFormatter(component_name, component_id, component_type)
        )
    else:
        file_handler.setFormatter(ServeFormatter(component_name, component_id))
    logger.addHandler(file_handler)


class LoggingContext:
    """
    Context manager to manage logging behaviors within a particular block, such as:
    1) Overriding logging level

    Source (python3 official documentation)
    https://docs.python.org/3/howto/logging-cookbook.html#using-a-context-manager-for-selective-logging # noqa: E501
    """

    def __init__(self, logger, level=None):
        self.logger = logger
        self.level = level

    def __enter__(self):
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)
