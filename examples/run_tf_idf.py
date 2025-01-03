import click
from compgraph.algorithms import inverted_index_graph
import json


def process_inverted_index(input_filepath: str, output_filepath: str) -> None:
    """
    Выполняет построение графа tf-idf

    :param input_filepath: Путь к входному файлу
    :param output_filepath: Путь к выходному файлу
    """
    graph = inverted_index_graph(
        input_stream_name=input_filepath,
        text_column='text',
        result_column='tf_idf',
        from_file=True
    )
    result = graph.run(input=lambda: input_filepath)

    with open(output_filepath, "w") as out:
        for row in result:
            json.dump(row, out)
            out.write("\n")


@click.command()
@click.option('--input-file', '-i', required=True, help="Путь к входному файлу с текстом")
@click.option('--output-file', '-o', required=True, help="Путь к выходному файлу для записи результата")
def main(input_file: str, output_file: str) -> None:
    """
    Основная функция для запуска скрипта из командной строки.
    """
    process_inverted_index(input_file, output_file)


if __name__ == "__main__":
    main()
