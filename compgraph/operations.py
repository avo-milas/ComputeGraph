import copy
from abc import abstractmethod, ABC
import typing as tp
from itertools import groupby
import re
import heapq
import math
from datetime import datetime

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Read(Operation):
    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        self.filename = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self.filename) as f:
            for line in f:
                yield self.parser(line)


class ReadIterFactory(Operation):
    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self.name]():
            yield row


# Operations


class Mapper(ABC):
    """Base class for mappers"""

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
            for mapped_row in self.mapper(row):
                yield mapped_row


class Reducer(ABC):
    """Base class for reducers"""

    @abstractmethod
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for key, group in groupby(rows, lambda x: [x[k] for k in self.keys]):
            for row in self.reducer(tuple(self.keys), group):
                yield row


class Joiner(ABC):
    """Base class for joiners"""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        table1 = groupby(rows, lambda x: [x[k] for k in self.keys])
        table2 = groupby(args[0], lambda x: [x[k] for k in self.keys])
        try:
            rp = next(iter(table2))
            table2_empty = False

            for lp_key, left_group in table1:

                while not table2_empty and rp[0] < lp_key:
                    for row in self.joiner(tuple(self.keys), [], rp[1]):
                        yield row
                    try:
                        rp = next(iter(table2))
                    except StopIteration:
                        table2_empty = True

                if table2_empty or lp_key < rp[0]:
                    for row in self.joiner(tuple(self.keys), left_group, []):
                        yield row
                elif lp_key == rp[0]:
                    for row in self.joiner(tuple(self.keys), left_group, rp[1]):
                        yield row
                    try:
                        rp = next(table2)
                    except StopIteration:
                        table2_empty = True

            while not table2_empty:
                for row in self.joiner(tuple(self.keys), [], rp[1]):
                    yield row
                try:
                    rp = next(iter(table2))
                except StopIteration:
                    table2_empty = True

        except (StopIteration, TypeError):
            for lp_key, left_group in table1:
                for row in self.joiner(tuple(self.keys), left_group, []):
                    yield row


# Dummy operators


class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield row
            break


# Mappers

class InvertValue(Mapper):
    """Invert value to sort"""

    def __init__(self, column: str, inv_value: str = 'inv_val'):
        """
        :param column: name of column to process
        :param inv_value: name of the inverted value column
        """
        self.column = column
        self.inv_value = inv_value

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.inv_value] = -row[self.column]
        yield row


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        self.regex = re.compile(r'[^\w\s]|_')
        row[self.column] = self.regex.sub('', row[self.column])
        # row[self.column] = re.sub(r'[^\w\s]|_', '', row[self.column])
        yield row


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
        row[self.column] = self._lower_case(row[self.column])
        yield row


class FilterOp(Mapper):
    """Filter rows based on the predicate"""

    def __init__(self, column: str, predicate: tp.Callable[[tp.Any], bool]):
        """
        :param column: name of column to process
        :param predicate: predicate to filter
        """
        self.column = column
        self.predicate = predicate

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.predicate(row[self.column]):
            yield row


class Split(Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        s = 0
        for sep in re.finditer(self.separator if self.separator is not None else r'\s+', row[self.column]):
            e = sep.start()
            new_row = copy.deepcopy(row)
            new_row[self.column] = row[self.column][s:e]
            s = sep.end()
            yield new_row
        # new_row = copy.deepcopy(row)
        new_row = {k: v for k, v in row.items()}
        new_row[self.column] = row[self.column][s:]
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
        ans = 1
        for col in self.columns:
            ans *= row[col]
        row[self.result_column] = ans
        yield row


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
        new_row = {}
        for k in self.columns:
            # if k in row.keys():
            new_row[k] = row[k]
        yield new_row


class IDF(Mapper):
    """Count idf based on this formula: ln((total number of docs) / (docs where word_i is present))"""

    def __init__(self, total_docs: str, docs_with_word: str, result_column: str = 'idf'):
        """
        :param total_docs: name of column with total number of docs
        :param docs_with_word: name of column with number of docs where word_i is present
        :param result_column: name of the result column
        """
        self.total_docs = total_docs
        self.docs_with_word = docs_with_word
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result_column] = math.log(row[self.total_docs] / row[self.docs_with_word])
        yield row


class PMI(Mapper):
    """Count pmi based on this formula:
    ln((frequency of word_i in doc_j) / (frequency of word_i in all documents combined))"""

    def __init__(self, tf_doc: str, tf_all_docs: str, result_column: str = 'pmi'):
        """
        :param tf_doc: name of column with term frequency of the word regarding doc_i
        :param tf_all_docs: name of column with term frequency of the word regarding all documents
        :param result_column: name of the result column
        """
        self.tf_doc = tf_doc
        self.tf_all_docs = tf_all_docs
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result_column] = math.log(row[self.tf_doc] / row[self.tf_all_docs])
        yield row


class HaversineDistance(Mapper):
    """Count haversine distance between 2 points"""

    def __init__(self, start: str, end: str, result_column: str = 'length'):
        """
        :param start: name of column with latitude and longitude of the start
        :param end:  name of column with latitude and longitude of the end
        :param result_column: name of the result column
        """
        self.start = start
        self.end = end
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        # if self.result_column not in row.keys():
        lat1, lon1, lat2, lon2 = map(math.radians, [row[self.start][1], row[self.start][0],
                                                    row[self.end][1], row[self.end][0]])
        delta_lat = lat2 - lat1
        delta_lon = lon2 - lon1
        radius = 6373

        term1 = (1 - math.cos(delta_lat)) / 2
        term2 = math.cos(lat1) * math.cos(lat2) * (1 - math.cos(delta_lon)) / 2
        distance = 2 * radius * math.asin(math.sqrt(term1 + term2))

        row[self.result_column] = distance
        yield row
        # else:
        #     row[self.result_column] /= 1000
        #     yield row


format_time_1 = "%Y%m%dT%H%M%S"  # Без миллисекунд
format_time_2 = "%Y%m%dT%H%M%S.%f"  # С миллисекундами


def parser_t(time_string: str) -> datetime:
    try:
        return datetime.strptime(time_string, format_time_2)
    except ValueError:
        return datetime.strptime(time_string, format_time_1)


class FindTime(Mapper):
    """Find the number of hours between 2 points in time"""

    def __init__(self, start_time: str, end_time: str, result_column: str = 'time_h'):
        """
        :param start_time: name of column with time of the start of the journey
        :param end_time: name of column with time of the end of the journey
        :param result_column: name of the result column
        """
        self.start_time = start_time
        self.end_time = end_time
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        datetime1 = parser_t(row[self.start_time])
        datetime2 = parser_t(row[self.end_time])

        time_difference = (datetime2 - datetime1).total_seconds() / 3600

        row[self.result_column] = time_difference
        yield row


# class FindSpeed(Mapper):
#     """Find the speed of the observation"""
#
#     def __init__(self, result_column: str = 'speed', time: str='time_h', length: str='length'):
#         """
#         :param time: name of column with time of the observation
#         :param length: name of column with length of the observation
#         :param result_column: name of the result column
#         """
#         self.result_column = result_column
#         self.time = time
#         self.length = length
#
#     def __call__(self, row: TRow) -> TRowsGenerator:
#         speed = (row[self.length] / row[self.time])
#         row[self.result_column] = speed
#         yield row

class ExtractDOWHour(Mapper):
    """Extract day of week and hour from the date"""

    def __init__(self, dttm: str, dow: str = 'weekday', hour: str = 'hour'):
        """
        :param dttm: name of the column with datetime of the start
        :param dow: name of the result-column with day of week
        :param hour: name of the result-column with hour
        """
        self.dttm = dttm
        self.dow = dow
        self.hour = hour

    def __call__(self, row: TRow) -> TRowsGenerator:
        dttm = parser_t(row[self.dttm])
        row[self.hour] = dttm.hour
        row[self.dow] = dttm.strftime('%a')
        yield row


# Reducers


class TopN(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        h: list[tuple[tp.Any, int, dict[str, tp.Any]]] = []
        heapq.heapify(h)
        i = 0
        for row in rows:
            if len(h) < self.n:
                heapq.heappush(h, (row[self.column_max], i, row))
            else:
                heapq.heappushpop(h, (row[self.column_max], i, row))
            i += 1
        for value, i, row in h:
            yield row


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        tf_dict = {}
        cnt = 0

        for row in rows:
            cnt += 1
            word = row[self.words_column]

            if word not in tf_dict:
                tf_dict[word] = {group_key_val: row[group_key_val] for group_key_val in group_key}
                tf_dict[word][self.words_column] = row[self.words_column]
                tf_dict[word][self.result_column] = 0

            tf_dict[word][self.result_column] += 1

        for word, row in tf_dict.items():
            row[self.result_column] /= cnt
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

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        cnt_dict = {}
        cnt = 0
        for row in rows:
            for group_key_val in group_key:
                cnt_dict[group_key_val] = row[group_key_val]
            cnt += 1
        cnt_dict[self.column] = cnt
        yield cnt_dict


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

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        sum_dict = {self.column: 0}
        for row in rows:
            for group_key_val in group_key:
                sum_dict[group_key_val] = row[group_key_val]
            sum_dict[self.column] += row[self.column]
        yield sum_dict


class Avg(Reducer):
    """
    Average of values aggregated by keys
    """

    def __init__(self, numerator: str, denominator: str, result_column: str) -> None:
        """
        :param numerator: name of column with values added up to numerator
        :param denominator: name of column with values added up to denominator
        :param result_column: name for avg column
        """
        self.numerator = numerator
        self.denominator = denominator
        self.result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        numerator_sum = 0
        denominator_sum = 0
        avg_dict = {}
        first_time = True
        for row in rows:
            if first_time:
                for group_key_val in group_key:
                    avg_dict[group_key_val] = row[group_key_val]
                first_time = False
            numerator_sum += row[self.numerator]
            denominator_sum += row[self.denominator]
        avg_dict[self.result_column] = numerator_sum / denominator_sum
        yield avg_dict


# Joiners


class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        try:
            row_a = next(iter(rows_a))
            try:
                row_b = next(iter(rows_b))
                saved_b = [row_b] + list(rows_b)
                a_empty = False
                while not a_empty:
                    for row_b in saved_b:
                        joined_row = {}
                        for keys_key in row_a.keys() & row_b.keys() & set(keys):
                            joined_row[keys_key] = row_a[keys_key]
                        for double_key in row_a.keys() & row_b.keys() - set(keys):
                            joined_row[f'{double_key}{self._a_suffix}'] = row_a[double_key]
                            joined_row[f'{double_key}{self._b_suffix}'] = row_b[double_key]
                        for a_key in row_a.keys() - row_b.keys():
                            joined_row[a_key] = row_a[a_key]
                        for b_key in row_b.keys() - row_a.keys():
                            joined_row[b_key] = row_b[b_key]
                        yield joined_row
                    try:
                        row_a = next(iter(rows_a))
                    except (TypeError, StopIteration):
                        a_empty = True
            except (TypeError, StopIteration):
                pass
        except (TypeError, StopIteration):
            pass


class OuterJoiner(Joiner):
    """Join with outer strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        try:
            row_a = next(iter(rows_a))
            try:
                row_b = next(iter(rows_b))
                saved_b = [row_b] + list(rows_b)
                a_empty = False
                while not a_empty:
                    for row_b in saved_b:
                        joined_row = {}
                        for keys_key in row_a.keys() & row_b.keys() & set(keys):
                            joined_row[keys_key] = row_a[keys_key]
                        for double_key in row_a.keys() & row_b.keys() - set(keys):
                            joined_row[f'{double_key}{self._a_suffix}'] = row_a[double_key]
                            joined_row[f'{double_key}{self._b_suffix}'] = row_b[double_key]
                        for a_key in row_a.keys() - row_b.keys():
                            joined_row[a_key] = row_a[a_key]
                        for b_key in row_b.keys() - row_a.keys():
                            joined_row[b_key] = row_b[b_key]
                        yield joined_row
                    try:
                        row_a = next(iter(rows_a))
                    except (TypeError, StopIteration):
                        a_empty = True
            except (TypeError, StopIteration):
                a_empty = False
                while not a_empty:
                    yield row_a
                    try:
                        row_a = next(iter(rows_a))
                    except (TypeError, StopIteration):
                        a_empty = True
        except (TypeError, StopIteration):
            for row in rows_b:
                yield row


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:

        try:
            row_a = next(iter(rows_a))
            try:
                row_b = next(iter(rows_b))
                saved_b = [row_b] + list(rows_b)
                a_empty = False
                while not a_empty:
                    for row_b in saved_b:
                        joined_row = {}
                        for keys_key in row_a.keys() & row_b.keys() & set(keys):
                            joined_row[keys_key] = row_a[keys_key]
                        for double_key in row_a.keys() & row_b.keys() - set(keys):
                            joined_row[f'{double_key}{self._a_suffix}'] = row_a[double_key]
                            joined_row[f'{double_key}{self._b_suffix}'] = row_b[double_key]
                        for a_key in row_a.keys() - row_b.keys():
                            joined_row[a_key] = row_a[a_key]
                        for b_key in row_b.keys() - row_a.keys():
                            joined_row[b_key] = row_b[b_key]
                        yield joined_row
                    try:
                        row_a = next(iter(rows_a))
                    except (TypeError, StopIteration):
                        a_empty = True
            except (TypeError, StopIteration):
                a_empty = False
                while not a_empty:
                    yield row_a
                    try:
                        row_a = next(iter(rows_a))
                    except (TypeError, StopIteration):
                        a_empty = True
        except (TypeError, StopIteration):
            pass


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:

        try:
            row_b = next(iter(rows_b))
            try:
                row_a = next(iter(rows_a))
                saved_a = [row_a] + list(rows_a)
                b_empty = False
                while not b_empty:
                    for row_a in saved_a:
                        joined_row = {}
                        for keys_key in row_a.keys() & row_b.keys() & set(keys):
                            joined_row[keys_key] = row_a[keys_key]
                        for double_key in row_a.keys() & row_b.keys() - set(keys):
                            joined_row[f'{double_key}{self._a_suffix}'] = row_a[double_key]
                            joined_row[f'{double_key}{self._b_suffix}'] = row_b[double_key]
                        for a_key in row_a.keys() - row_b.keys():
                            joined_row[a_key] = row_a[a_key]
                        for b_key in row_b.keys() - row_a.keys():
                            joined_row[b_key] = row_b[b_key]
                        yield joined_row
                    try:
                        row_b = next(iter(rows_b))
                    except (TypeError, StopIteration):
                        b_empty = True
            except (TypeError, StopIteration):
                b_empty = False
                while not b_empty:
                    yield row_b
                    try:
                        row_b = next(iter(rows_b))
                    except (TypeError, StopIteration):
                        b_empty = True
        except (TypeError, StopIteration):
            pass
