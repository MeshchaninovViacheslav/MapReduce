from abc import abstractmethod, ABC
from itertools import groupby
import typing as tp

from .operation import TRowsIterable, TRowsGenerator, Operation, TRow


class Joiner(ABC):
    """Base class for joiners"""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass

    def _join_two_rows(self, keys: tp.Sequence[str], row_a: TRow, row_b: TRow) -> TRowsGenerator:
        intersected_keys: tp.Set[str] = set(row_a) & set(row_b) - set(keys)
        joined_row: tp.Dict[str, TRow] = dict()
        # append keys of row from first table
        for key in row_a:
            if key in intersected_keys:
                joined_row[key + self._a_suffix] = row_a[key]
            else:
                joined_row[key] = row_a[key]
        # append keys of row from second table
        for key in row_b:
            if key in intersected_keys:
                joined_row[key + self._b_suffix] = row_b[key]
            else:
                joined_row[key] = row_b[key]
        yield joined_row


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        assert len(args) > 0, "There is no key in args"

        gen_groups_first_table = groupby(rows, key=lambda row: tuple(row[key] for key in self.keys))
        gen_groups_second_table = groupby(args[0], key=lambda row: tuple(row[key] for key in self.keys))

        # initialize iterators of first and second tables
        key_group_first_table, rows_iter_first_table = next(gen_groups_first_table, (tuple(), None))
        key_group_second_table, rows_iter_second_table = next(gen_groups_second_table, (tuple(), None))
        rows_iter_default: tp.Iterable[tp.Dict[str, tp.Any]] = iter({})

        # mergesort two tables
        while rows_iter_first_table is not None and rows_iter_second_table is not None:
            if key_group_first_table < key_group_second_table:
                yield from self.joiner(self.keys, rows_iter_first_table, rows_iter_default)
                key_group_first_table, rows_iter_first_table = next(gen_groups_first_table, (tuple(), None))
            elif key_group_first_table > key_group_second_table:
                yield from self.joiner(self.keys, rows_iter_default, rows_iter_second_table)
                key_group_second_table, rows_iter_second_table = next(gen_groups_second_table, (tuple(), None))
            else:
                yield from self.joiner(self.keys, rows_iter_first_table, rows_iter_second_table)
                key_group_first_table, rows_iter_first_table = next(gen_groups_first_table, (tuple(), None))
                key_group_second_table, rows_iter_second_table = next(gen_groups_second_table, (tuple(), None))

        # merge remaining elements of first table
        while rows_iter_first_table is not None:
            yield from self.joiner(self.keys, rows_iter_first_table, rows_iter_default)
            _, rows_iter_first_table = next(gen_groups_first_table, (tuple(), None))

        # merge remaining elements of second table
        while rows_iter_second_table is not None:
            yield from self.joiner(self.keys, rows_iter_default, rows_iter_second_table)
            _, rows_iter_second_table = next(gen_groups_second_table, (tuple(), None))


# Joiners
class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        if rows_a and rows_b:
            content_of_rows_a = rows_a
            content_of_rows_b = list(rows_b)
            for row_a in content_of_rows_a:
                for row_b in content_of_rows_b:
                    yield from self._join_two_rows(keys, row_a, row_b)


class OuterJoiner(Joiner):
    """Join with outer strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        content_of_rows_a = list(rows_a) or [{}]
        content_of_rows_b = list(rows_b) or [{}]

        for row_a in content_of_rows_a:
            for row_b in content_of_rows_b:
                yield from self._join_two_rows(keys, row_a, row_b)


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        content_of_rows_a = rows_a
        content_of_rows_b = list(rows_b) or [{}]

        for row_a in content_of_rows_a:
            for row_b in content_of_rows_b:
                yield from self._join_two_rows(keys, row_a, row_b)


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        content_of_rows_a = list(rows_a) or [{}]
        content_of_rows_b = rows_b
        for row_a in content_of_rows_b:
            for row_b in content_of_rows_a:
                yield from self._join_two_rows(keys, row_a, row_b)
