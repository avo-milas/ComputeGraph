import click
from compgraph.algorithms import word_count_graph
import json


def process_word_count(input_filepath: str, output_filepath: str) -> None:
    """
    Выполняет построение графа подсчёта слов

    :param input_filepath: Путь к входному файлу
    :param output_filepath: Путь к выходному файлу
    """
    graph = word_count_graph(
        input_stream_name=input_filepath,
        text_column='text',
        count_column='count',
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
    process_word_count(input_file, output_file)


if __name__ == "__main__":
    main()
