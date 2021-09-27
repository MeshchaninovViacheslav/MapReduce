import typing as tp

from .operations import ExternalSort, ReadIterFactory, Read, TRow, Reducer, Reduce, Joiner, TRowsGenerator, Mapper
from .operations import Map, Join

tOperation = tp.Optional[tp.Union[
    ReadIterFactory,
    Read,
    Map, Reduce, Join, ExternalSort]]


class Graph:
    """Computational graph implementation"""

    def __init__(self) -> None:
        self._input = [self]
        self._operation: tOperation = None

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        graph = Graph()
        graph._operation = ReadIterFactory(name)
        graph._input = []
        return graph

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], TRow]) -> 'Graph':
        """Construct new graph extended with operation for reading rows from file
        Use ops.Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        graph = Graph()
        graph._operation = Read(filename, parser)
        graph._input = []
        return graph

    def map(self, mapper: Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        graph = Graph()
        graph._operation = Map(mapper)
        graph._input = [self]
        return graph

    def reduce(self, reducer: Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        graph = Graph()
        graph._operation = Reduce(reducer, keys)
        graph._input = [self]
        return graph

    def sort(self, keys: tp.Sequence[str], reverse: bool = False) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        graph = Graph()
        graph._operation = ExternalSort(keys, reverse)
        graph._input = [self]
        return graph

    def join(self, joiner: Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        graph = Graph()
        graph._operation = Join(joiner, keys)
        graph._input = [self, join_graph]
        return graph

    def run(self, **kwargs: tp.Any) -> TRowsGenerator:
        """Single method to start execution; data sources passed as kwargs"""
        if not self._input:
            return self._operation(**kwargs)  # type: ignore
        if self._operation is not None:
            stream = self._input[0].run(**kwargs)
            if len(self._input) < 2:
                return self._operation(stream)
            return self._operation(stream, self._input[1].run(**kwargs))
        else:
            raise Exception("No generator in graph")
