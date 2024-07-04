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

* `previsao_chegada.Rmd`: Gera o arquivo `previsao_chegada.html`, apresentação de slides com explicação do método e resultados.

* `relatorio.Rmd`: Gera o relatório `relatorio.pdf` entregue.

## Instruções de uso

1. O arquivo `projecao.sql` tem os placeholders `{start_date}` e `{end_date}` que devem ser substituídos por datas de início e fim.

2. Para gerar a base de validação, o código em SQL pode ser executado através do console do BigQuery, ou utilizando o script `download_dados.R`. Só é necessário adaptar os diretórios e a ID do projeto do BigQuery utilizados.

3. Tendo baixado a base de validação, o script `download_dados.R` também separa os dados em um arquivo por linha de ônibus. Não é necessário, mas facilita o treinamento dos modelos.

4. Basta configurar os diretórios e rodar o script `modelos_previsao.R`, que treina todos os modelos e salva os resultados de desempenho out-of-sample.
