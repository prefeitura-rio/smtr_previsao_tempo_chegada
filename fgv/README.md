# Field Project - FGV EMAp & SMTR - Previsão de Tempo de Chegada

## Introdução
Este projeto visa processar dados de GPS e GTFS para análise de rotas e veículos. A pipeline de pré-processamento envolve várias etapas que garantem a limpeza, filtragem e enriquecimento dos dados, permitindo análises e visualizações detalhadas feitas posteriormente utilizando a linguagem R.

## Estrutura do Projeto
- `preprocess_data.py`: Script Python que realiza o pré-processamento dos dados de GPS e GTFS, resultando em novos arquivos CSVs com os dados filtrados e com novas features.
- `preprocess_example.ipynb`: Notebook para execução e visualização do processo de pré-processamento. Apresenta uma execução interativa do pipeline, com visualizações que exibem os resultados de cada etapa.
- `requirements.txt`: Lista de dependências necessárias para executar o projeto em Python.
- `model.R`: Script em R que realiza um novo tratamento dos dados de um arquivo CSV e avalia um modelo de regressão linear generalizada.
- `model_report.rmd`: Relatório em R Markdown que descreve o processo de modelagem e avaliação do modelo de regressão linear generalizada.

### Diretórios
- `data/gps_data`: Contém os arquivos CSV de dados de GPS. Os arquivos são resultados de queries no banco de dados (BigQuery), contendo as informações dos ônibus em movimento.
- `data/gtfs_data`: Contém os arquivos TXT de dados de GTFS. Os arquivos são obtidos do site da Prefeitura do Rio de Janeiro e contêm informações sobre as rotas e paradas de ônibus. Esses arquivos servem de referência e devem ser mantidos atualizados conforme a disponibilidade de novos dados.
- `src/gps_handler.py`: Módulo Python que contém a classe `GPSHandler`, responsável por carregar, processar e visualizar os dados de GPS.
- `src/gtfs_handler.py`: Módulo Python que contém a classe `GTFSHandler`, responsável por carregar, processar e visualizar os dados de GTFS.
- `src/utils.py`: Módulo Python que contém funções utilitárias para o pré-processamento dos dados, otimizadas para a execução do pipeline através de funções modulares, numpy e numba (JIT).

# Pré-processamento

## Como executar

Para instalar as dependências necessárias, execute:
```bash
pip install -r requirements.txt
```

Tendo instalado as dependências, mova os arquivos TXT do GTFS para a pasta `data/gtfs_data` e os arquivos CSV do GPS, obtidos através de queries no BigQuery, para a pasta `data/gps_data`. Em seguida, execute o script `preprocess_data.py` para realizar o pré-processamento dos dados.

```bash
python preprocess_data.py
```

## Pipeline de Pré-processamento
A seguir estão descritas as etapas do pipeline de pré-processamento implementado no arquivo `preprocess_data.py`:

### 1. Definição de variáveis e diretórios
```python
# Define se os arquivos de saída devem ser sobrescritos, ou se o processamento deve pular arquivos já processados e existentes
OVERWRITE = True

# Define limites para o número de pontos em uma rota para um ônibus ser considerado
# Nesse caso, serão considerados apenas ônibus com mais de 300 pontos e menos de 2880 pontos (para um dia de operação de 24 horas)
MIN_POINTS = 300
MAX_POINTS = 2880

# Define diretórios para os arquivos de GTFS e GPS
GTFS_FOLDER = "./data/gtfs_data"
GPS_FOLDER = "./data/gps_data"

# Define um diretório de saída para os arquivos processados
OUTPUT_FOLDER = "./data/output/"
```
Essas configurações podem ser adaptadas conforme a necessidade do usuário. Contudo, outros parâmetros podem ser ajustados diretamente no código, como, por exemplo:
- A tolerância para a filtragem de coordenadas de GPS (`tolerance_meters`, na função `filter_gps_coordinates`). Esse valor corresponde à distância máxima entre a rota e a coordenada de GPS para que a coordenada seja considerada válida ("dentro da rota").
- A tolerância para a inferência de direções de GPS (`tolerance`, na função `infer_bus_direction`). Esse valor corresponde à diferença entre as distâncias percorridas em direções opostas para que a direção seja considerada válida, buscando inferir direções apenas quando um movimento significativo e de direção clara é detectado.
- O número de amostras a serem consideradas para a inferência de direções (`N`, na função `assign_direction`). Esse valor corresponde ao número de amostras de GPS a serem consideradas para a inferência de direções, buscando inferir direções apenas quando um movimento significativo e de direção clara é detectado. Por padrão, são consideradas as 5 últimas amostras de GPS.
- A tolerância para a inferência de direções de GPS (`terminal_tolerance`, na função `assign_direction`). Esse valor corresponde à distância máxima de um terminal (início ou fim da rota) para que mudanças na direção sejam permitidas. Por padrão, caso o ônibus esteja a mais de 500 metros de distância de um terminal, mudanças na direção não são permitidas e, portanto, a direção é considerada constante.

### 2. Carregamento dos Dados
```python
gtfs = gtfs_handler.GTFSHandler(GTFS_FOLDER)
gps = gps_handler.GPSHandler(GPS_FOLDER)
```
Inicializa os manipuladores de dados GTFS e GPS, carregando os dados dos diretórios especificados.

### 3. Verificação e Divisão de Arquivos de GPS
```python
gps.split_file(GPS_FOLDER, file)
```
Verifica se os arquivos de GPS estão no formato correto (`YYYY-MM-DD.csv`). Se não estiverem, divide-os em arquivos com a mesma data e remove o arquivo original.

### 4. Processamento de Dados por Rota e Veículo
```python
gps.get_route_data(route)
gtfs.filter_by_route(str(route))
```
Após a sepração por dias, os dados são segmentados por rota e, para isso, são identificadas as rotas presentes nos dados de GPS e filtrados os dados de GTFS para a rota em questão.

### 5. Plotagem dos Dados de GPS e Rota
```python
gtfs.plot_route(title=f"Route {route}", save_path=route_output_path + f"route_{route}.png")
gps.plot_gps_data(title=f"GPS data from bus {vehicle} (route {route})", save_path=bus_output_path + "gps_data.png")
```
Os dados de GPS e as rotas são plotados para visualização. Embora sirva apenas como um ferramenta de verificação manual, a plotagem dos dados pode ser útil para identificar problemas nos dados de GTFS, principalmente indicando a má interpretação de rotas e paradas, o que acontece, por exemplo, sobre rotas com trechos reversíveis.

### 6. Filtragem de Dados do GPS segundo os ônibus
```python
gps.get_bus_data(bus)   
```
Filtra as coordenadas de GPS com base em cada ônibus, permitindo o processamento individual de cada veículo.

### 7. Processamento de Dados de GPS
```python
utils.process_bus_data(gps, gtfs, bus, route, bus_output_path)
```
Corresponde ao núcleo do pipeline de pré-processamento, onde os dados de GPS são processados e enriquecidos com informações de GTFS. As etapas de processamento incluem:

#### 7.1. Plotagem dos Dados de GPS
```python
gps.plot_gps_data(title=f"GPS data from bus {vehicle} (route {route})", save_path=bus_output_path + "gps_data.png")
```
Esta etapa envolve a plotagem dos dados de GPS do ônibus para visualização inicial.

#### 7.2. Filtragem de Coordenadas de GPS
```python
gps.filter_gps_coordinates(gtfs)
```
Os dados de GPS são filtrados com base na proximidade da rota, fornecida pelo GTFS.

#### 7.3. Plotagem dos Dados Filtrados de GPS
```python
gps.plot_gps_data(gps.gps_df[gps.gps_df["in_route"] == True], gtfs.route_shape_segments, title=f"Filtered GPS data from bus {vehicle} (route {route})", save_path=bus_output_path + "filtered_gps_data.png")
```
Os dados de GPS filtrados são plotados novamente para verificar a precisão da filtragem.

#### 7.4. Atribuição de Distâncias a Partir do Início da Rota
```python
gps.get_distance_from_start(gtfs)
```
Calcula a distância de cada ponto de GPS desde o início da rota em ambas as direções, ou seja, a quantos metros de distância do início e do fim da rota o ônibus se encontra.

#### 7.5. Atribuição de Direções e Inferência de Direções
```python
gps.gps_df['direction'], gps.gps_df['direction_directly_infered'] = assign_direction(gps.gps_df['in_route'].to_numpy(), gps.gps_df['distance_from_start_0'].to_numpy(), gps.gps_df['distance_from_start_1'].to_numpy(), N=3)
```
Atribui direções inferidas a cada ponto de GPS com base na rota e na distância percorrida em cada direção. Observa as últimas 3 amostras de GPS para inferir a direção do ônibus, buscando por movimentos significativos e de direção clara.

#### 7.6. Conversão de Timestamps
```python
gps.gps_df['timestamp_gps'] = pd.to_datetime(gps.gps_df['timestamp_gps'])
gps.gps_df['timestamp_gps_seconds'] = gps.gps_df['timestamp_gps'].astype(np.int64) // 10**9
```
Converte os timestamps para o formato datetime e cria uma nova coluna em segundos.

#### 7.7. Atribuição de Distâncias Percorridas
```python
gps.gps_df['distance_traveled'], gps.gps_df['cumulative_distance_traveled'], gps.gps_df['time_traveled'], gps.gps_df['cumulative_time_traveled']  = assign_distance_traveled(gps.gps_df['timestamp_gps_seconds'].to_numpy(), gps.gps_df['in_route'].to_numpy(), gps.gps_df['direction'].to_numpy(), gps.gps_df['distance_from_start_0'].to_numpy(), gps.gps_df['distance_from_start_1'].to_numpy())
```
Calcula a distância percorrida e o tempo para cada ponto de GPS com base na direção inferida.

#### 7.8. Obtenção de Paradas por Direção
```python
gtfs.get_stops_by_direction()
```
Obtém as paradas de ônibus organizadas para cada direção.

#### 7.9. Atribuição de Paradas aos Dados de GPS
```python
gps.gps_df['last_stop_index'], gps.gps_df['next_stop_index'], gps.gps.gps_df['last_stop_distance'], gps.gps_df['next_stop_distance'] = assign_stops(gps.gps_df['in_route'].to_numpy(), gps.gps_df['direction'].to_numpy(), gps.gps_df['distance_traveled'].to_numpy(), gtfs.stops_distances_by_direction)
```
Atribui a última parada e a próxima parada a cada ponto de GPS com base na distância percorrida e na direção inferida

#### 7.10. Cálculo da Velocidade Média
```python
gps.gps_df['mean_speed_1_min'] = assign_mean_speed(gps.gps_df['in_route'].to_numpy(), gps.gps_df['timestamp_gps_seconds'].to_numpy(), gps.gps_df['cumulative_distance_traveled'].to_numpy(), N=1)
gps.gps_df['mean_speed_3_min'] = assign_mean_speed(gps.gps_df['in_route'].to_numpy(), gps.gps_df['timestamp_gps_seconds'].to_numpy(), gps.gps_df['cumulative_distance_traveled'].to_numpy(), N=3)
gps.gps_df['mean_speed_5_min'] = assign_mean_speed(gps.gps_df['in_route'].to_numpy(), gps.gps_df['timestamp_gps_seconds'].to_numpy(), gps.gps_df['cumulative_distance_traveled'].to_numpy(), N=5)
```
Calcula a velocidade média do ônibus em intervalos de 1, 3 e 5 minutos, gerando velocidades médias para janelas móveis de tempo para além dos 10 minutos, calculados por padrão.

#### 7.11. Geração de Pontos Virtuais de Validação
```python
gps.validation_df = virtualize_stop_points(gps.gps_df['timestamp_gps'].to_numpy(), gps.gps_df['in_route'].to_numpy(), gps.gps_df['direction'].to_numpy(), gps.gps_df['last_stop_index'].to_numpy(), gps.gps_df['next_stop_index'].to_numpy(), gps.gps_df['distance_traveled'].to_numpy(), gps.gps_df['cumulative_distance_traveled'].to_numpy(), gps.gps_df['cumulative_time_traveled'].to_numpy(), gps.gps.gps_df[['mean_speed_1_min', 'mean_speed_3_min', 'mean_speed_5_min']].to_numpy(), gtfs.stops_distances_by_direction, vehicle, route)
```
Gera pontos virtuais para cada parada de ônibus, simulando o tempo em que o ônibus pararia em cada localização.
O que chamamos de "pontos virtuais" ou "pontos de validação" correspondem a interpolações lineares sobre as paradas dos ônibus, ou seja, pontos que representam o tempo em que o ônibus estaria em cada parada, considerando a velocidade média do ônibus e o tempo de viagem entre as paradas. Esses pontos são construídos pois os pontos de GPS podem não corresponder exatamente às paradas de ônibus, e, portanto, a validação dos resultados depende de pontos que representem o tempo em que o ônibus estaria em cada parada. Contudo, o uso desses pontos virtuais para validação deve ser cauteloso, dado que possuem uma alta correlação com os pontos originais que deram origem a eles.

#### 7.12. Salvamento dos Resultados
```python
gps.gps_df.to_csv(bus_output_path + "raw_processed_gps_data.csv", index=False)
gps.gps_df[gps.gps_df['in_route'] == True].to_csv(bus_output_path + "processed_gps_data.csv", index=False)
gps.validation_df.to_csv(bus_output_path + "validation_data.csv", index=False)
```
Salva os dados processados em arquivos CSV para posterior análise.
A fim de arquivar alguns conjuntos de dados intermediários, são salvos os seguintes arquivos:
- `raw_processed_gps_data.csv`: Dados de GPS processados, incluindo todas as etapas de processamento.
- `processed_gps_data.csv`: Dados de GPS processados, filtrados para pontos dentro da rota (excluindo distantes da rota).
- `validation_data.csv`: Dados de validação, contendo apenas os pontos virtuais gerados para cada parada de ônibus.

Além disso, fora do escopo da função `process_bus_data`, os dados de GPS filtrados e de validação são agregados em um único arquivo CSV, que armazena os dados por rota, a fim de facilitar o desenvolvimento de modelos individuais para cada rota. Essa rotina, implementada ao final do arquivo `preprocess_data.py`, é responsável por concatenar os dados de GPS filtrados e de validação para cada rota e salvar o resultado em um arquivo CSV.

# Modelagem

## Como executar

Primeiramente, é necessário instalar as dependências necessárias para a execução do script em R. Para isso, execute:

```R
install.packages("tidyverse")
```

Tendo instalado as dependências, defina o caminho do arquivo CSV com os dados tratados (variável `SOURCE_DATA_DIR`) e o caminho do arquivo CSV com os dados tratados (variável `TREATED_DATA_DIR`) no script `model.R`. Em seguida, execute o script para realizar o tratamento dos dados e avaliação do modelo de regressão linear generalizada.

```bash
Rscript model.R
```

## Pipeline de Tratamento 
O arquivo `model.R` é um script em R que processa os dados de um arquivo csv e avalia um modelo de regressão linear generalizada com os seguintes preditores: distância percorrida, direção, velocidade média em 5 minutos, se é final de semana ou não e o período do dia.

O script possui duas funções principais:

### 1. `treat_data`

A função `treat_data` recebe o caminho do arquivo csv com os dados e o número da rota e retorna os dados tratados. Os dados tratados são salvos em um arquivo csv na pasta definida pela variável `TREATED_DATA_DIR`. Além disso, a função salva três gráficos na pasta `plots`:

- Um gráfico de dispersão do tempo de viagem vs distância percorrida para viagens em que o ônibus estava parado (*dead trips*).
- Um gráfico de dispersão do tempo de viagem vs distância percorrida para viagens em que o ônibus estava no meio da rota (*middle trips*).
- Um gráfico de dispersão do tempo de viagem vs distância percorrida para viagens válidas.

Os thresholds para considerar uma viagem inválida são definidos e podem ser alterados nas seguintes variáveis:

- `wrong_direction_count`: número mínimo de pontos de dados para considerar uma viagem com direção errada (padrão = 5).

- `dead_trips_count`: número mínimo de pontos de dados para considerar uma viagem morta (padrão = 20).

- `dead_trips_threshold`: limite de distância percorrida para considerar uma viagem morta (padrão = 100).

- `max_distance_traveled`: limite de distância percorrida para considerar uma viagem no meio da rota (padrão = 2000).

### 2. `evaluate_model`

A função `evaluate_model` recebe o caminho do arquivo csv com os dados e o número da rota e retorna o AIC e o RMSE do modelo de regressão linear generalizada. Além disso, a função salva três gráficos na pasta `plots`:

- Um gráfico de caixa dos erros do modelo de período de tempo para cada dia da semana.
- Um gráfico de caixa dos erros do modelo de período de tempo para cada dia da semana e cada intervalo de 500 metros percorridos.
- Um gráfico de dispersão do tempo de viagem previsto vs real.

## Exemplo de Uso

```R
# Tratamento dos dados
filtered_data <- treat_data("409_train_data.csv", "409")

# Salva os dados tratados em um arquivo csv
write.csv(filtered_data, "409_train_data_treated.csv", row.names = FALSE)

# Avaliação do modelo
metrics <- evaluate_model("409_train_data_treated.csv", "409")

# Printa as métricas do modelo
print(metrics)
```

## Conclusão
Este projeto oferece uma pipeline robusta para o pré-processamento de dados de GPS e GTFS, facilitando análises detalhadas de rotas e veículos. Além disso, o modelo de regressão linear generalizada implementado em R permite avaliar o tempo de viagem de ônibus com base em variáveis como distância percorrida, direção, velocidade média, dia da semana e período do dia. Siga as etapas descritas para preparar seus dados e realizar suas análises.