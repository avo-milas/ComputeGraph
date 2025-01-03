import typing as tp

from . import operations as ops
from . import external_sort as ext_sort


def join_table2(joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> ops.Operation:
    class JoinExecutedTable2(ops.Operation):
        def __call__(self, rows: ops.TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> ops.TRowsGenerator:
            return ops.Join(joiner, keys)(rows, join_graph.run(**kwargs))

    return JoinExecutedTable2()


class Graph:
    """Computational graph implementation"""
    factory: ops.Operation
    instructions: list[ops.Operation] = []

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        graph = Graph()
        graph.factory = ops.ReadIterFactory(name)
        return graph

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> 'Graph':
        """Construct new graph extended with operation for reading rows from file
        Use ops.Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        graph = Graph()
        graph.factory = ops.Read(filename, parser)
        return graph

    @staticmethod
    def graph_from_another_graph(another_graph: 'Graph') -> 'Graph':
        graph = Graph()
        graph.factory = another_graph.factory
        graph.instructions = another_graph.instructions[:]
        return graph

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        graph = Graph.graph_from_another_graph(self)
        graph.instructions.append(ops.Map(mapper))
        return graph

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        graph = Graph.graph_from_another_graph(self)
        graph.instructions.append(ops.Reduce(reducer, keys))
        return graph

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        graph = Graph.graph_from_another_graph(self)
        graph.instructions.append(ext_sort.ExternalSort(keys))
        return graph

    def join(self, joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        graph = Graph.graph_from_another_graph(self)
        graph.instructions.append(join_table2(joiner, join_graph, keys))
        return graph

    def run(self, **kwargs: tp.Any) -> ops.TRowsIterable:
        """Single method to start execution; data sources passed as kwargs"""
        tmp_result = self.factory(**kwargs)
        for instruction in self.instructions:
            tmp_result = instruction(tmp_result, **kwargs)
        return tmp_result
