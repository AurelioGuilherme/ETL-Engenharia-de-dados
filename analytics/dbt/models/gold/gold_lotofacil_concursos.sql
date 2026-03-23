with concursos_tipados as (
    select *
    from {{ ref('silver_lotofacil') }}
),

concursos_enriquecidos as (
    select
        ingestion_id,
        ingested_at,
        source_file_path,
        source_file_hash,
        source_row_number,
        concurso,
        data_sorteio,
        bola1,
        bola2,
        bola3,
        bola4,
        bola5,
        bola6,
        bola7,
        bola8,
        bola9,
        bola10,
        bola11,
        bola12,
        bola13,
        bola14,
        bola15,
        ganhadores_15_acertos,
        cidade_uf,
        rateio_15_acertos,
        ganhadores_14_acertos,
        rateio_14_acertos,
        ganhadores_13_acertos,
        rateio_13_acertos,
        ganhadores_12_acertos,
        rateio_12_acertos,
        ganhadores_11_acertos,
        rateio_11_acertos,
        acumulado_15_acertos,
        arrecadacao_total,
        estimativa_premio,
        acumulado_sorteio_especial_lotofacil_da_independencia,
        observacao,
        (bola1 + bola2 + bola3 + bola4 + bola5 + bola6 + bola7 + bola8 + bola9 + bola10 + bola11 + bola12 + bola13 + bola14 + bola15) as soma_dezenas,
        ((bola1 % 2) + (bola2 % 2) + (bola3 % 2) + (bola4 % 2) + (bola5 % 2) + (bola6 % 2) + (bola7 % 2) + (bola8 % 2) + (bola9 % 2) + (bola10 % 2) + (bola11 % 2) + (bola12 % 2) + (bola13 % 2) + (bola14 % 2) + (bola15 % 2)) as quantidade_dezenas_impares,
        (15 - ((bola1 % 2) + (bola2 % 2) + (bola3 % 2) + (bola4 % 2) + (bola5 % 2) + (bola6 % 2) + (bola7 % 2) + (bola8 % 2) + (bola9 % 2) + (bola10 % 2) + (bola11 % 2) + (bola12 % 2) + (bola13 % 2) + (bola14 % 2) + (bola15 % 2))) as quantidade_dezenas_pares,
        (ganhadores_11_acertos + ganhadores_12_acertos + ganhadores_13_acertos + ganhadores_14_acertos + ganhadores_15_acertos) as total_ganhadores_faixas,
        ganhadores_15_acertos = 0 as acumulou,
        case
            when ganhadores_15_acertos > 0 then round(rateio_15_acertos / ganhadores_15_acertos, 2)
            else null
        end as premio_medio_por_ganhador_15,
        case
            when nullif(trim(cidade_uf), '') is null then 0
            else cardinality(regexp_split_to_array(regexp_replace(cidade_uf, '\s*;\s*', ';', 'g'), ';'))
        end as quantidade_localidades_ganhadoras_15,
        lag(arrecadacao_total) over (order by concurso) as arrecadacao_total_concurso_anterior,
        arrecadacao_total - lag(arrecadacao_total) over (order by concurso) as variacao_arrecadacao_total,
        avg(arrecadacao_total) over (
            order by concurso
            rows between 11 preceding and current row
        ) as media_movel_12_concursos_arrecadacao,
        rank() over (order by rateio_15_acertos desc nulls last) as ranking_rateio_15_acertos,
        extract(year from data_sorteio)::int as ano_concurso,
        acumulado_sorteio_especial_lotofacil_da_independencia > 0 as possui_acumulado_especial
    from concursos_tipados
)

select *
from concursos_enriquecidos
