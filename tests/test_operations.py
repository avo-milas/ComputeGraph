from compgraph.operations import parser_t
from datetime import datetime


def test_parser() -> None:
    unique_time = '20171024T183523'
    assert parser_t(unique_time) == datetime(2017, 10, 24, 18, 35, 23)
