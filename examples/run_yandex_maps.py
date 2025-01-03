import click
from compgraph.algorithms import yandex_maps_graph
import json


def process_yandex_maps(input_filepath_time: str, input_filepath_len: str, output_filepath: str) -> None:
    """
    Выполняет построение графа для Яндекс карт и возвращает результат.

    :param input_filepath_time: Путь к файлу с данными времени
    :param input_filepath_len: Путь к файлу с данными длины дорог
    :param output_filepath: Путь к выходному файлу
    """
    graph = yandex_maps_graph(
        input_stream_name_time=input_filepath_time,
        input_stream_name_length=input_filepath_len,
        enter_time_column='enter_time',
        leave_time_column='leave_time',
        edge_id_column='edge_id',
        start_coord_column='start',
        end_coord_column='end',
        weekday_result_column='weekday',
        hour_result_column='hour',
        speed_result_column='speed',
        from_file1=True,
        from_file2=True
    )

    result = graph.run(travel_time=lambda: input_filepath_time, edge_length=lambda: input_filepath_len)

    with open(output_filepath, "w") as out:
        for row in result:
            json.dump(row, out)
            out.write("\n")


@click.command()
@click.option('--input-file-time', '-it', required=True, help="Путь к входному файлу с данными времени")
@click.option('--input-file-len', '-il', required=True, help="Путь к входному файлу с данными длины дорог")
@click.option('--output-file', '-o', required=True, help="Путь к выходному файлу для записи результата")
def main(input_file_time: str, input_file_len: str, output_file: str) -> None:
    """
    Основная функция для запуска скрипта из командной строки.
    """
    process_yandex_maps(input_file_time, input_file_len, output_file)


if __name__ == "__main__":
    main()
