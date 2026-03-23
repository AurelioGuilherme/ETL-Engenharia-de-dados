select *
from {{ ref('gold_lotofacil_concursos') }}
where arrecadacao_total < 0
   or rateio_15_acertos < 0
   or ganhadores_15_acertos < 0
