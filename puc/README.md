O repositório consiste nos arquivos:

* `projecao.sql`:
        1. Junta dados de GPS e GTFS do data lake;
        2. Projeta a posição de cada ônibus em seu itinerário para calcular a distância viajada e identificar o `shape_id` que o ônibus está seguindo;
        3. Identifica quais os pontos seguintes no trajeto de cada ônibus;
        4. Calcula os dados históricos de quantos minutos os ônibus levaram para chegar até cada ponto, na variável `arrival_time`.
        
* `download_dados.R`: Usa a interface do BigQuery no R para ler os dados gerados pelo `projecao.sql` em cada dia, e armazena os dados para cada serviço em um arquivo `.csv` separado.

* `modelos_previsão.R`: Estima os modelos de (i) Médias históricas, (ii) Random Forest, e (iii) Rede Neural para cada linha na base de treino, e avalia seu desempenho na base de teste.

* `avaliacao_desempenho.R`: Produz tabelas e gráficos a partir dos resultados dos modelos.

* `/output/`: Armazena os resultados do desempenho dos modelos, e outras estatísticas descritivas.