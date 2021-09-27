import calendar
import re
from abc import abstractmethod, ABC
from copy import deepcopy
from datetime import datetime
import typing as tp

from .operation import TRow, TRowsIterable, TRowsGenerator, Operation


class Mapper(ABC):
    """Base class for mappers"""

    def __init__(self) -> None:
        pass

    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            yield from self.mapper(row)


# Dummy operators


class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


# Mappers


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column
        self._filter = re.compile('[^a-zA-Z ]')

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = deepcopy(row)
        new_row[self.column] = self._filter.sub('', new_row[self.column])
        yield new_row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = deepcopy(row)
        new_row[self.column] = self._lower_case(new_row[self.column])
        yield new_row


class RenameColumn(Mapper):
    """Rename column"""

    def __init__(self, column: str, new_column: str):
        """
        :param column: name of column to process
        :param new_column: new name of column
        """
        self.column = column
        self.new_column = new_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = deepcopy(row)
        del new_row[self.column]
        new_row[self.new_column] = row[self.column]
        yield new_row


class Split(Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: tp.Optional[str] = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator
        self._filter = re.compile(r'\W+')

    def __call__(self, row: TRow) -> TRowsGenerator:
        words = self._filter.split(row[self.column])

        for word in words:
            new_row = deepcopy(row)
            new_row[self.column] = word
            yield new_row


class Product(Mapper):
    """Calculates product of multiple columns"""

    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = deepcopy(row)
        new_row[self.result_column] = 1 if self.columns else 0
        for column in self.columns:
            new_row[self.result_column] *= new_row[column]
        yield new_row


class ArithmeticProcedureForMultipleColumns(Mapper):
    """Calculates operation of two columns"""

    def __init__(self, operation: tp.Callable[[tp.Any, tp.Any], tp.Any],
                 columns: tp.Sequence[str], result_column: str) -> None:
        """
        :param operation: operation on two columns
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column
        self.operation = operation

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = deepcopy(row)
        new_row[self.result_column] = self.operation(*[new_row[column] for column in self.columns])
        yield new_row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {column: deepcopy(row[column]) if column in row else None for column in self.columns}


class MakeDatetime(Mapper):
    """Make datetime from string format time"""

    @staticmethod
    def string_to_datetime(string_time: str) -> datetime:
        return datetime.strptime(string_time, "%Y%m%dT%H%M%S.%f")

    def __init__(self, time_column: str, datetime_column: str) -> None:
        """
        :param time_column: name of column with time to save
        :param datetime_column: name of column with date
        """
        self.time_column = time_column
        self.datetime_column = datetime_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = deepcopy(row)
        new_row[self.datetime_column] = self.string_to_datetime(new_row[self.time_column])
        yield new_row


class ProcessDuration(Mapper):
    """Process timedelta in hours"""

    def __init__(self, enter_time_column: str,
                 leave_time_column: str,
                 duration: str) -> None:
        """
        :param enter_time_column: time of start
        :param leave_time_column: time of finish
        :param duration: duration
        """
        self.enter_time_column = enter_time_column
        self.leave_time_column = leave_time_column
        self.duration = duration

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = deepcopy(row)
        new_row[self.duration] = (new_row[self.leave_time_column] -
                                  new_row[self.enter_time_column]).total_seconds() / 3600
        yield new_row


class MakeWeekdayHour(Mapper):
    """Create weekday and hour column"""

    def __init__(self, datetime_column: str, weekday_result_column: str,
                 hour_result_column: str) -> None:
        """
            :param datetime_column: initial datetime
            :param weekday_result_column: weekday of date
            :param hour_result_column: hour of date
        """
        self.datetime_column = datetime_column
        self.weekday_result_column = weekday_result_column
        self.hour_result_column = hour_result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        new_row = deepcopy(row)
        new_row[self.weekday_result_column] = calendar.day_abbr[new_row[self.datetime_column].weekday()]
        new_row[self.hour_result_column] = new_row[self.datetime_column].hour
        yield new_row
