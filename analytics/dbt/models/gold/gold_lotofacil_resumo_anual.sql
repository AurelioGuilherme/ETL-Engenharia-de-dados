select
    ano_concurso,
    count(*) as quantidade_concursos,
    sum(arrecadacao_total) as arrecadacao_total_ano,
    avg(arrecadacao_total) as arrecadacao_media_concurso,
    max(rateio_15_acertos) as maior_rateio_15_acertos,
    avg(ganhadores_15_acertos) as media_ganhadores_15_acertos,
    sum(case when acumulou then 1 else 0 end) as quantidade_concursos_acumulados,
    avg(quantidade_localidades_ganhadoras_15) as media_localidades_ganhadoras_15
from {{ ref('gold_lotofacil_concursos') }}
group by ano_concurso
