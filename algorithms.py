import math
from copy import deepcopy
import typing as tp

from . import Graph
from .operations import FilterPunctuation, LowerCase, Split, Count, FirstReducer, ArithmeticProcedureForMultipleColumns
from .operations import TermFrequency, Product, Project, TopN, Filter, Sum, RenameColumn
from .operations import InnerJoiner, MakeDatetime, MakeWeekdayHour, ProcessDuration


def haversine_distance(point1: tp.Tuple[float, float],
                       point2: tp.Tuple[float, float]) -> float:
    """Calculate the distance in kilometers between two points on the earth"""
    EARTH_RADIUS_IN_KM = 6373
    longitude1, latitude1, longitude2, latitude2 = map(math.radians, [*point1, *point2])
    angle = 2 * math.asin(math.sqrt(
        math.sin((latitude2 - latitude1) / 2) ** 2 + math.cos(latitude1) * math.cos(latitude2) * math.sin(
            (longitude2 - longitude1) / 2) ** 2))
    return angle * EARTH_RADIUS_IN_KM


def word_count_graph(input_stream_name: str, text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    return Graph.graph_from_iter(input_stream_name) \
        .map(FilterPunctuation(text_column)) \
        .map(LowerCase(text_column)) \
        .map(Split(text_column)) \
        .sort([text_column]) \
        .reduce(Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf') -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""
    # Split input stream to words
    graph_split_words = Graph.graph_from_iter(input_stream_name) \
        .map(FilterPunctuation(text_column)) \
        .map(LowerCase(text_column)) \
        .map(Split(text_column))

    # Count number of docs
    number_docs_column = 'number_docs'
    graph_number_docs = Graph.graph_from_iter(input_stream_name) \
        .reduce(Count(number_docs_column), list())

    # Count number of docs according to word and compute idf
    number_docs_by_word_column = 'number_docs_by_word'
    idf_column = 'idf'
    graph_count_idf = graph_split_words \
        .sort([doc_column, text_column]) \
        .reduce(FirstReducer(), [doc_column, text_column]) \
        .sort([text_column]) \
        .reduce(Count(number_docs_by_word_column), [text_column]) \
        .join(InnerJoiner(), graph_number_docs, tuple()) \
        .map(ArithmeticProcedureForMultipleColumns(lambda x, y: math.log(x / y),
                                                   [number_docs_column, number_docs_by_word_column],
                                                   idf_column))

    # Compute tf
    tf_column = 'tf'
    graph_count_tf = graph_split_words.sort([doc_column]) \
        .reduce(TermFrequency(text_column), [doc_column])

    # joining tf_graph and idf_graph and computing tf-idf
    graph_count_tfidf = graph_count_tf.sort([text_column]) \
        .join(InnerJoiner(), graph_count_idf, [text_column]) \
        .map(Product([tf_column, idf_column], result_column)) \
        .map(Project([doc_column, text_column, result_column])) \
        .sort([text_column]) \
        .reduce(TopN(result_column, 3), [text_column])

    return graph_count_tfidf


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
              result_column: str = 'pmi') -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""

    # Calculate the number of times the word_i in the doc_j and delete those that do not meet the condition
    number_wordi_doci_column = 'number_wordi_doci'
    graph_number_wordi_doci = Graph.graph_from_iter(input_stream_name) \
        .map(FilterPunctuation(text_column)) \
        .map(LowerCase(text_column)) \
        .map(Split(text_column)) \
        .sort([text_column]) \
        .reduce(Count(number_wordi_doci_column), [text_column, doc_column]) \
        .map(Filter(lambda row: row[number_wordi_doci_column] >= 2 and len(row[text_column]) > 4)) \
        .sort([doc_column])

    # Calculate the number of words in each document
    number_words_doci_column = 'number_words_doci'
    graph_number_words_doci = deepcopy(graph_number_wordi_doci) \
        .sort([doc_column]) \
        .reduce(Sum(number_wordi_doci_column), [doc_column]) \
        .map(RenameColumn(number_wordi_doci_column, number_words_doci_column))

    # Calculate the frequency of words in all documents
    frequency_wordi_doci_column = 'frequency_wordi_doci'
    graph_frequency_word = graph_number_wordi_doci \
        .join(InnerJoiner(), graph_number_words_doci, [doc_column]) \
        .map(ArithmeticProcedureForMultipleColumns(lambda x, y: x / y,
                                                   [number_wordi_doci_column, number_words_doci_column],
                                                   frequency_wordi_doci_column)) \
        .map(Project([text_column, doc_column, frequency_wordi_doci_column])) \
        .sort([text_column])

    # Calculate the number of words in all documents
    number_words_in_all_docs_column = 'number_words_in_all_docs'
    graph_number_words_in_all_docs = deepcopy(graph_number_wordi_doci) \
        .reduce(Sum(number_wordi_doci_column), list()) \
        .map(RenameColumn(number_wordi_doci_column, number_words_in_all_docs_column))

    # Calculate the frequency of word in all documents
    number_wordi_in_all_docs_column = 'number_wordi_in_all_docs'
    frequency_wordi_in_all_docs_column = 'frequency_wordi_in_all_docs'
    graph_number_wordi_in_all_docs = deepcopy(graph_number_wordi_doci) \
        .reduce(Sum(number_wordi_doci_column), [text_column]) \
        .map(RenameColumn(number_wordi_doci_column, number_wordi_in_all_docs_column)) \
        .join(InnerJoiner(), graph_number_words_in_all_docs, list()) \
        .map(ArithmeticProcedureForMultipleColumns(lambda x, y: x / y,
                                                   [number_wordi_in_all_docs_column,
                                                    number_words_in_all_docs_column],
                                                   frequency_wordi_in_all_docs_column)) \
        .map(Project([text_column, frequency_wordi_in_all_docs_column])) \
        .sort([text_column])

    # Calculate pmi
    pmi_column = 'pmi'
    graph = graph_frequency_word.join(InnerJoiner(), graph_number_wordi_in_all_docs, [text_column]) \
        .map(ArithmeticProcedureForMultipleColumns(lambda x, y: math.log(x / y),
                                                   [frequency_wordi_doci_column,
                                                    frequency_wordi_in_all_docs_column],
                                                   pmi_column)) \
        .sort([doc_column]) \
        .reduce(TopN(pmi_column, n=10), [doc_column]) \
        .map(Project([doc_column, text_column, pmi_column])) \
        .sort([pmi_column], reverse=True) \
        .sort([doc_column])
    return graph


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed') -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""
    graph_time = Graph.graph_from_iter(input_stream_name_time)
    graph_length = Graph.graph_from_iter(input_stream_name_length)

    enter_datetime_column = 'enter_datetime'
    leave_datetime_column = 'leave_datetime'

    duration_column = 'duration'
    distance_column = 'distance'

    # Calculate weekday and hour of input time
    graph_time = graph_time \
        .map(MakeDatetime(enter_time_column, enter_datetime_column)) \
        .map(MakeDatetime(leave_time_column, leave_datetime_column)) \
        .map(MakeWeekdayHour(enter_datetime_column, weekday_result_column, hour_result_column)) \
        .map(ProcessDuration(enter_datetime_column, leave_datetime_column, duration_column)) \
        .map(Project([edge_id_column, weekday_result_column, hour_result_column, duration_column])) \
        .sort([edge_id_column])

    # Calculate haversine distance
    graph_length = graph_length \
        .map(ArithmeticProcedureForMultipleColumns(lambda start, end: haversine_distance(start, end),
                                                   [start_coord_column, end_coord_column],
                                                   distance_column)) \
        .map(Project([edge_id_column, distance_column])) \
        .sort([edge_id_column])

    # join tables and calculate result speed
    graph = graph_time.join(InnerJoiner(), graph_length, [edge_id_column]) \
        .sort([weekday_result_column, hour_result_column])

    graph_length = graph.reduce(Sum(duration_column), [weekday_result_column, hour_result_column])
    graph_time = graph.reduce(Sum(distance_column), [weekday_result_column, hour_result_column])
    graph = graph_length.join(InnerJoiner(), graph_time, [weekday_result_column, hour_result_column]) \
        .map(ArithmeticProcedureForMultipleColumns(lambda dist, time: dist / time,
                                                   [distance_column, duration_column],
                                                   speed_result_column)) \
        .map(Project([weekday_result_column, hour_result_column, speed_result_column]))
    return graph
