import click
from compgraph.algorithms import pmi_graph
import json


def process_pmi(input_filepath: str, output_filepath: str) -> None:
    """
    Выполняет построение графа PMI

    :param input_filepath: Путь к входному файлу
    :param output_filepath: Путь к выходному файлу
    """
    graph = pmi_graph(
        input_stream_name=input_filepath,
        doc_column='doc_id',
        text_column='text',
        result_column='pmi',
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
    process_pmi(input_file, output_file)


if __name__ == "__main__":
    main()
