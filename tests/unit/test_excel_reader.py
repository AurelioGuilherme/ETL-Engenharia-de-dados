from potatocore.ingestion.excel_reader import normalize_column_name, parse_brl_currency


def test_normalize_column_name() -> None:
    assert normalize_column_name("Acumulado sorteio especial Lotofácil da Independência") == (
        "acumulado_sorteio_especial_lotofacil_da_independencia"
    )


def test_parse_brl_currency() -> None:
    assert parse_brl_currency("R$ 1.234,56") == 1234.56
    assert parse_brl_currency("texto") == "texto"
