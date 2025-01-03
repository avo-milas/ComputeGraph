from compgraph.algorithms import word_count_graph
import tempfile
import json


def test_multiple_calls_graph_file() -> None:
    docs1 = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
    ]

    expected1 = [
        {'count': 1, 'text': 'hello'},
        {'count': 1, 'text': 'little'},
        {'count': 1, 'text': 'my'},
        {'count': 1, 'text': 'world'}
    ]

    docs2 = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    expected2 = [
        {'count': 1, 'text': 'hell'},
        {'count': 1, 'text': 'world'},
        {'count': 2, 'text': 'hello'},
        {'count': 2, 'text': 'my'},
        {'count': 3, 'text': 'little'}
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        input_filepath = f"{temp_dir}/inp_tmpr.txt"
        graph = word_count_graph(input_stream_name=input_filepath, text_column='text', count_column='count',
                                 from_file=True)

        with open(input_filepath, "w") as f:
            for doc in docs1:
                json.dump(doc, f)
                f.write("\n")

        result1 = graph.run(text=lambda: input_filepath)
        assert list(result1) == expected1

        with open(input_filepath, "w") as f:
            for doc in docs2:
                json.dump(doc, f)
                f.write("\n")

        result2 = graph.run(text=lambda: input_filepath)
        assert list(result2) == expected2
