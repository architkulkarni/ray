import builtins
from typing import Any, Generic, List, Callable, Union, Tuple

import numpy as np

import ray
from ray.types import ObjectRef
from ray.experimental.data.block import Block, BlockAccessor, \
    BlockMetadata, T
from ray.experimental.data.impl.arrow_block import ArrowRow
from ray.util.annotations import PublicAPI

WriteResult = Any


@PublicAPI(stability="beta")
class Datasource(Generic[T]):
    """Interface for defining a custom ``ray.data.Dataset`` datasource.

    To read a datasource into a dataset, use ``ray.data.read_datasource()``.
    To write to a writable datasource, use ``Dataset.write_datasource()``.

    See ``RangeDatasource`` and ``DummyOutputDatasource`` for examples
    of how to implement readable and writable datasources.
    """

    def prepare_read(self, parallelism: int,
                     **read_args) -> List["ReadTask[T]"]:
        """Return the list of tasks needed to perform a read.

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should be as close to this value as possible.
            read_args: Additional kwargs to pass to the datasource impl.

        Returns:
            A list of read tasks that can be executed to read blocks from the
            datasource in parallel.
        """
        raise NotImplementedError

    def prepare_write(self, blocks: List[ObjectRef[Block]],
                      metadata: List[BlockMetadata],
                      **write_args) -> List["WriteTask[T]"]:
        """Return the list of tasks needed to perform a write.

        Args:
            blocks: List of data block references. It is recommended that one
                write task be generated per block.
            metadata: List of block metadata.
            write_args: Additional kwargs to pass to the datasource impl.

        Returns:
            A list of write tasks that can be executed to write blocks to the
            datasource in parallel.
        """
        raise NotImplementedError

    def on_write_complete(self, write_tasks: List["WriteTask[T]"],
                          write_task_outputs: List[WriteResult],
                          **kwargs) -> None:
        """Callback for when a write job completes.

        This can be used to "commit" a write output. This method must
        succeed prior to ``write_datasource()`` returning to the user. If this
        method fails, then ``on_write_failed()`` will be called.

        Args:
            write_tasks: The list of the original write tasks.
            write_task_outputs: The list of write task outputs.
            kwargs: Forward-compatibility placeholder.
        """
        pass

    def on_write_failed(self, write_tasks: List["WriteTask[T]"],
                        error: Exception, **kwargs) -> None:
        """Callback for when a write job fails.

        This is called on a best-effort basis on write failures.

        Args:
            write_tasks: The list of the original write tasks.
            error: The first error encountered.
            kwargs: Forward-compatibility placeholder.
        """
        pass


@PublicAPI(stability="beta")
class ReadTask(Callable[[], Block]):
    """A function used to read a block of a dataset.

    Read tasks are generated by ``datasource.prepare_read()``, and return
    a ``ray.data.Block`` when called. Metadata about the read operation can
    be retrieved via ``get_metadata()`` prior to executing the read.

    Ray will execute read tasks in remote functions to parallelize execution.
    """

    def __init__(self, read_fn: Callable[[], Block], metadata: BlockMetadata):
        self._metadata = metadata
        self._read_fn = read_fn

    def get_metadata(self) -> BlockMetadata:
        return self._metadata

    def __call__(self) -> Block:
        return self._read_fn()


@PublicAPI(stability="beta")
class WriteTask(Callable[[], WriteResult]):
    """A function used to write a chunk of a dataset.

    Write tasks are generated by ``datasource.prepare_write()``, and return
    a datasource-specific output that is passed to ``on_write_complete()``
    on write completion.

    Ray will execute write tasks in remote functions to parallelize execution.
    """

    def __init__(self, write_fn: Callable[[], WriteResult]):
        self._write_fn = write_fn

    def __call__(self) -> WriteResult:
        return self._write_fn()


class RangeDatasource(Datasource[Union[ArrowRow, int]]):
    """An example datasource that generates ranges of numbers from [0..n).

    Examples:
        >>> source = RangeDatasource()
        >>> ray.data.read_datasource(source, n=10).take()
        ... [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    """

    def prepare_read(self,
                     parallelism: int,
                     n: int,
                     block_format: str = "list",
                     tensor_shape: Tuple = (1, )) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        block_size = max(1, n // parallelism)

        # Example of a read task. In a real datasource, this would pull data
        # from an external system instead of generating dummy data.
        def make_block(start: int, count: int) -> Block:
            if block_format == "arrow":
                return pyarrow.Table.from_arrays(
                    [np.arange(start, start + count)], names=["value"])
            elif block_format == "tensor":
                return np.ones(
                    tensor_shape, dtype=np.int64) * np.expand_dims(
                        np.arange(start, start + count),
                        tuple(range(1, 1 + len(tensor_shape))))
            else:
                return list(builtins.range(start, start + count))

        i = 0
        while i < n:
            count = min(block_size, n - i)
            if block_format == "arrow":
                import pyarrow
                schema = pyarrow.Table.from_pydict({"value": [0]}).schema
            elif block_format == "tensor":
                schema = {"dtype": "int64", "shape": (None, ) + tensor_shape}
            elif block_format == "list":
                schema = int
            else:
                raise ValueError("Unsupported block type", block_format)
            read_tasks.append(
                ReadTask(
                    lambda i=i, count=count: make_block(i, count),
                    BlockMetadata(
                        num_rows=count,
                        size_bytes=8 * count,
                        schema=schema,
                        input_files=None)))
            i += block_size

        return read_tasks


class DummyOutputDatasource(Datasource[Union[ArrowRow, int]]):
    """An example implementation of a writable datasource for testing.

    Examples:
        >>> output = DummyOutputDatasource()
        >>> ray.data.range(10).write_datasource(output)
        >>> assert output.num_ok == 1
    """

    def __init__(self):
        # Setup a dummy actor to send the data. In a real datasource, write
        # tasks would send data to an external system instead of a Ray actor.
        @ray.remote
        class DataSink:
            def __init__(self):
                self.rows_written = 0
                self.enabled = True

            def write(self, block: Block) -> str:
                block = BlockAccessor.for_block(block)
                if not self.enabled:
                    raise ValueError("disabled")
                self.rows_written += block.num_rows()
                return "ok"

            def get_rows_written(self):
                return self.rows_written

            def set_enabled(self, enabled):
                self.enabled = enabled

        self.data_sink = DataSink.remote()
        self.num_ok = 0
        self.num_failed = 0

    def prepare_write(self, blocks: List[ObjectRef[Block]],
                      metadata: List[BlockMetadata],
                      **write_args) -> List["WriteTask[T]"]:
        tasks = []
        for b in blocks:
            tasks.append(
                WriteTask(lambda b=b: ray.get(self.data_sink.write.remote(b))))
        return tasks

    def on_write_complete(self, write_tasks: List["WriteTask[T]"],
                          write_task_outputs: List[WriteResult]) -> None:
        assert len(write_task_outputs) == len(write_tasks)
        assert all(w == "ok" for w in write_task_outputs), write_task_outputs
        self.num_ok += 1

    def on_write_failed(self, write_tasks: List["WriteTask[T]"],
                        error: Exception) -> None:
        self.num_failed += 1
