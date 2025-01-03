from . import Graph, operations
import json


def word_count_graph(input_stream_name: str, text_column: str = 'text',
                     count_column: str = 'count', from_file: bool = False) -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    if from_file:
        g = Graph.graph_from_file(input_stream_name, json.loads)
    else:
        g = Graph.graph_from_iter(input_stream_name)

    return g.map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf', from_file: bool = False) -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""
    if from_file:
        g = Graph.graph_from_file(input_stream_name, json.loads)
    else:
        g = Graph.graph_from_iter(input_stream_name)

    split_word = g.map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))

    count_docs = g.reduce(operations.Count('count_docs'), tuple([]))

    count_idf = split_word.sort([doc_column, text_column]) \
        .reduce(operations.FirstReducer(), [doc_column, text_column]) \
        .sort([text_column]) \
        .reduce(operations.Count('count_docs_word'), [text_column]) \
        .join(operations.InnerJoiner(), count_docs, []) \
        .map(operations.IDF('count_docs', 'count_docs_word', 'idf')) \
        .sort([text_column])

    tf = split_word.sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column), [doc_column]) \

    tf_idf = tf.sort([text_column]) \
        .join(operations.InnerJoiner(), count_idf, [text_column]) \
        .map(operations.Product(('tf', 'idf'), result_column)) \
        .sort([text_column]) \
        .reduce(operations.TopN(result_column, 3), [text_column]) \
        .map(operations.Project([doc_column, text_column, result_column]))

    return tf_idf


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
              result_column: str = 'pmi', from_file: bool = False) -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""
    if from_file:
        g = Graph.graph_from_file(input_stream_name, json.loads)
    else:
        g = Graph.graph_from_iter(input_stream_name)

    filtered_words = g.map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .map(operations.FilterOp(text_column, (lambda x: len(x) > 4))) \

    words_count = filtered_words.sort([doc_column, text_column]) \
        .reduce(operations.Count('count_word_in_doc'), [doc_column, text_column]) \

    filtered_count = filtered_words.sort([doc_column, text_column]) \
        .join(operations.InnerJoiner(), words_count.sort([doc_column, text_column]), [doc_column, text_column]) \
        .map(operations.FilterOp('count_word_in_doc', (lambda x: x >= 2))) \
        .map(operations.Project([doc_column, text_column]))

    tf_all_docs = filtered_count.reduce(operations.TermFrequency(text_column, 'tf_all_docs'), []) \

    tf_docs = filtered_count.sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column, 'tf_doc'), [doc_column]) \

    pmi = tf_docs.sort([text_column]) \
        .join(operations.InnerJoiner(), tf_all_docs.sort([text_column]), [text_column]) \
        .map(operations.PMI('tf_doc', 'tf_all_docs', result_column)) \
        .sort([doc_column]) \
        .reduce(operations.TopN(result_column, 10), [doc_column]) \
        .map(operations.InvertValue(result_column)) \
        .sort([doc_column, 'inv_val']) \
        .map(operations.Project([doc_column, text_column, result_column]))

    return pmi


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed', from_file1: bool = False, from_file2: bool = False) -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""
    if from_file1:
        g_time = Graph.graph_from_file(input_stream_name_time, json.loads)
    else:
        g_time = Graph.graph_from_iter(input_stream_name_time)

    if from_file2:
        g_length = Graph.graph_from_file(input_stream_name_length, json.loads)
    else:
        g_length = Graph.graph_from_iter(input_stream_name_length)

    length_roads = g_length.map(operations.HaversineDistance(start_coord_column, end_coord_column)) \
        .map(operations.Project([edge_id_column, 'length'])) \
        .sort([edge_id_column]) \

    journeys = g_time.map(operations.FindTime(enter_time_column, leave_time_column)) \
        .map(operations.ExtractDOWHour(enter_time_column, weekday_result_column, hour_result_column)) \
        .sort([edge_id_column]) \
        .join(operations.LeftJoiner(), length_roads, [edge_id_column]) \
        .map(operations.Project([weekday_result_column, hour_result_column, 'length', 'time_h'])) \
        .sort([weekday_result_column, hour_result_column]) \
        .reduce(operations.Avg('length', 'time_h', speed_result_column), [weekday_result_column, hour_result_column]) \
        .map(operations.Project([weekday_result_column, hour_result_column, speed_result_column]))

    return journeys
