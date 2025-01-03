import subprocess
import tempfile
import json
from pathlib import Path
from pytest import approx
from operator import itemgetter


def test_word_count_cli() -> None:
    docs1 = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
    ]

    expected1 = [
        {'count': 1, 'text': 'hello'},
        {'count': 1, 'text': 'little'},
        {'count': 1, 'text': 'my'},
        {'count': 1, 'text': 'world'},
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        input_filepath = Path(temp_dir) / "inp_tmpr.txt"
        output_filepath = Path(temp_dir) / "out_tmpr.txt"

        with open(input_filepath, "w") as f:
            for doc in docs1:
                json.dump(doc, f)
                f.write("\n")

        subprocess.run(
            [
                "python",
                "examples/run_word_count.py",
                "--input-file",
                str(input_filepath),
                "--output-file",
                str(output_filepath),
            ],
            check=True,
        )

        with open(output_filepath, "r") as f:
            result = [json.loads(line.strip()) for line in f]

        assert result == expected1


def test_tf_idf_cli() -> None:
    rows = [
        {'doc_id': 1, 'text': 'hello, little world'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 3, 'text': 'little little little'},
        {'doc_id': 4, 'text': 'little? hello little world'},
        {'doc_id': 5, 'text': 'HELLO HELLO! WORLD...'},
        {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!!'}
    ]

    expected = [
        {'doc_id': 1, 'text': 'hello', 'tf_idf': approx(0.1351, 0.001)},
        {'doc_id': 1, 'text': 'world', 'tf_idf': approx(0.1351, 0.001)},

        {'doc_id': 2, 'text': 'little', 'tf_idf': approx(0.4054, 0.001)},

        {'doc_id': 3, 'text': 'little', 'tf_idf': approx(0.4054, 0.001)},

        {'doc_id': 4, 'text': 'hello', 'tf_idf': approx(0.1013, 0.001)},
        {'doc_id': 4, 'text': 'little', 'tf_idf': approx(0.2027, 0.001)},

        {'doc_id': 5, 'text': 'hello', 'tf_idf': approx(0.2703, 0.001)},
        {'doc_id': 5, 'text': 'world', 'tf_idf': approx(0.1351, 0.001)},

        {'doc_id': 6, 'text': 'world', 'tf_idf': approx(0.3243, 0.001)}
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        input_filepath = Path(temp_dir) / "inp_tmpr.txt"
        output_filepath = Path(temp_dir) / "out_tmpr.txt"

        with open(input_filepath, "w") as f:
            for doc in rows:
                json.dump(doc, f)
                f.write("\n")

        subprocess.run(
            [
                "python",
                "examples/run_tf_idf.py",
                "--input-file",
                str(input_filepath),
                "--output-file",
                str(output_filepath),
            ],
            check=True,
        )

        with open(output_filepath, "r") as f:
            result = [json.loads(line.strip()) for line in f]

        assert sorted(result, key=itemgetter('doc_id', 'text')) == expected


def test_pmi_cli() -> None:
    rows = [
        {'doc_id': 1, 'text': 'hello, little world'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 3, 'text': 'little little little'},
        {'doc_id': 4, 'text': 'little? hello little world'},
        {'doc_id': 5, 'text': 'HELLO HELLO! WORLD...'},
        {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!! HELLO!!!!!!!'}
    ]

    expected = [  # Mind the order !!!
        {'doc_id': 3, 'text': 'little', 'pmi': approx(0.9555, 0.001)},
        {'doc_id': 4, 'text': 'little', 'pmi': approx(0.9555, 0.001)},
        {'doc_id': 5, 'text': 'hello', 'pmi': approx(1.1786, 0.001)},
        {'doc_id': 6, 'text': 'world', 'pmi': approx(0.7731, 0.001)},
        {'doc_id': 6, 'text': 'hello', 'pmi': approx(0.0800, 0.001)},
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        input_filepath = Path(temp_dir) / "inp_tmpr.txt"
        output_filepath = Path(temp_dir) / "out_tmpr.txt"

        with open(input_filepath, "w") as f:
            for doc in rows:
                json.dump(doc, f)
                f.write("\n")

        subprocess.run(
            [
                "python",
                "examples/run_pmi.py",
                "--input-file",
                str(input_filepath),
                "--output-file",
                str(output_filepath),
            ],
            check=True,
        )

        with open(output_filepath, "r") as f:
            result = [json.loads(line.strip()) for line in f]

        assert result == expected


def test_yandex_maps_cli() -> None:
    lengths = [
        {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953],
         'edge_id': 8414926848168493057},
        {'start': [37.524768467992544, 55.88785375468433], 'end': [37.52415172755718, 55.88807155843824],
         'edge_id': 5342768494149337085},
        {'start': [37.56963176652789, 55.846845586784184], 'end': [37.57018438540399, 55.8469259692356],
         'edge_id': 5123042926973124604},
        {'start': [37.41463478654623, 55.654487907886505], 'end': [37.41442892700434, 55.654839486815035],
         'edge_id': 5726148664276615162},
        {'start': [37.584684155881405, 55.78285809606314], 'end': [37.58415022864938, 55.78177368734032],
         'edge_id': 451916977441439743},
        {'start': [37.736429711803794, 55.62696328852326], 'end': [37.736344216391444, 55.626937723718584],
         'edge_id': 7639557040160407543},
        {'start': [37.83196756616235, 55.76662947423756], 'end': [37.83191015012562, 55.766647034324706],
         'edge_id': 1293255682152955894},
    ]

    times = [
        {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171011T145553.040000', 'enter_time': '20171011T145551.957000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171020T090548.939000', 'enter_time': '20171020T090547.463000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171024T144101.879000', 'enter_time': '20171024T144059.102000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171022T131828.330000', 'enter_time': '20171022T131820.842000',
         'edge_id': 5342768494149337085},
        {'leave_time': '20171014T134826.836000', 'enter_time': '20171014T134825.215000',
         'edge_id': 5342768494149337085},
        {'leave_time': '20171010T060609.897000', 'enter_time': '20171010T060608.344000',
         'edge_id': 5342768494149337085},
        {'leave_time': '20171027T082600.201000', 'enter_time': '20171027T082557.571000',
         'edge_id': 5342768494149337085}
    ]

    expected = [
        {'weekday': 'Fri', 'hour': 8, 'speed': approx(62.2322, 0.001)},
        {'weekday': 'Fri', 'hour': 9, 'speed': approx(78.1070, 0.001)},
        {'weekday': 'Fri', 'hour': 11, 'speed': approx(88.9552, 0.001)},
        {'weekday': 'Sat', 'hour': 13, 'speed': approx(100.9690, 0.001)},
        {'weekday': 'Sun', 'hour': 13, 'speed': approx(21.8577, 0.001)},
        {'weekday': 'Tue', 'hour': 6, 'speed': approx(105.3901, 0.001)},
        {'weekday': 'Tue', 'hour': 14, 'speed': approx(41.5145, 0.001)},
        {'weekday': 'Wed', 'hour': 14, 'speed': approx(106.4505, 0.001)}
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        input_filepath_time = Path(temp_dir) / "inp_tmpr_time.txt"
        input_filepath_len = Path(temp_dir) / "inp_tmpr_len.txt"
        output_filepath = Path(temp_dir) / "out_tmpr.txt"

        with open(input_filepath_time, "w") as f:
            for doc in times:
                json.dump(doc, f)
                f.write("\n")

        with open(input_filepath_len, "w") as f:
            for doc in lengths:
                json.dump(doc, f)
                f.write("\n")

        subprocess.run(
            [
                "python",
                "examples/run_yandex_maps.py",
                "--input-file-time",
                str(input_filepath_time),
                "--input-file-len",
                str(input_filepath_len),
                "--output-file",
                str(output_filepath),
            ],
            check=True,
        )

        with open(output_filepath, "r") as f:
            result = [json.loads(line.strip()) for line in f]

        assert sorted(result, key=itemgetter('weekday', 'hour')) == expected
