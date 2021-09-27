import heapq
from abc import abstractmethod, ABC
from copy import deepcopy
from itertools import groupby
from operator import itemgetter
import typing as tp

from .operation import TRowsIterable, TRowsGenerator, Operation


class Reducer(ABC):
    """Base class for reducers"""

    @abstractmethod
    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for _, grouper in groupby(rows, key=lambda row: tuple(row[key] for key in self.keys)):
            yield from self.reducer(tuple(self.keys), grouper)


# Reducers


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield row
            break


class TopN(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in heapq.nlargest(self.n, rows, key=itemgetter(self.column_max)):
            yield row


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column: str = words_column
        self.result_column: str = result_column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        new_rows: tp.Dict[tp.Tuple[str, ...], tp.Any] = dict()
        number_rows = 0
        for row in rows:
            number_rows += 1
            word = row[self.words_column]
            if word in new_rows:
                new_rows[word][self.result_column] += 1
            else:
                new_rows[word] = {key: row[key] for key in [*group_key, self.words_column]}
                new_rows[word][self.result_column] = 1

        for row in new_rows.values():
            row[self.result_column] /= number_rows
            yield row


class Count(Reducer):
    """
    Count records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        new_row: tp.Dict[str, int] = dict()
        new_row.setdefault(self.column, 0)
        for row in rows:
            if group_key and group_key[0] not in new_row:
                new_row.update({key: deepcopy(row[key]) for key in group_key})
            new_row[self.column] += 1
        yield new_row


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        new_row: tp.Dict[str, int] = dict()
        new_row.setdefault(self.column, 0)
        for row in rows:
            if group_key and not group_key[0] in new_row:
                new_row.update({key: deepcopy(row[key]) for key in group_key})
            new_row[self.column] += row[self.column]
        yield new_row
