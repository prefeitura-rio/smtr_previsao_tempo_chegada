library(dplyr)
library(lubridate)
library(basedosdados)
library(jsonlite)
library(googleCloudStorageR)
library(data.table)

# projeto google cloud
set_billing_id("absolute-text-417919") 

# caminho para os dados mais pesados
source <- "F:/Dados/SMTR"

###########################
## 1) Bases de validação ##
###########################

# código SQL que puxa os dados de gps,
# identifica as viagens,
# identifica os pontos,
# e calcula tempos de chegada realizados

query <- readr::read_file("projecao.sql")

# montando lista de dias para ler

start_date <- "2024-05-01"

end_date <- "2024-05-31"

dates <- seq(ymd(start_date), ymd(end_date), "days")

dates <- paste0("\"", dates, "\"")

for (data in dates) {
    
    message(data)
    
    # query para ler o dia específico
    
    q <- query %>%
        gsub("\\{start_date\\}", data, .) %>%
        gsub("\\{end_date\\}", data, .)
    
    bigrquery::bq_auth(
        email = "igor.rilave@hotmail.com"
    )
    
    table <- bigrquery::bq_project_query(
        "absolute-text-417919",
        query = q
    )
    
    data <- data %>%
        stringr::str_remove_all("\"")
    
    bigrquery::bq_table_save(
        table,
        destination_uris = paste0("gs://base_validacao/bus-", data, "-*.json")
    )
}

# para baixar da cloud storage, rodar
# gsutil -m cp -r gs://base_validacao F:/Dados/SMTR/raw

# separando por linha

files <- list.files(
    file.path(source, "raw"),
    pattern = "*.json",
    full.names = TRUE
)

for (file in files) {
    message(file)
    
    # Read the file into a data.table
    lines <- fread(file, sep = "\n", header = FALSE, col.names = "json")
    
    # Parse each line as JSON and combine them into a data.table
    df <- rbindlist(lapply(lines$json, fromJSON), fill = TRUE)
    
    rm(lines)
    
    # separando por linha
    
    df <- split(df, df$servico)
    
    # escrevendo cada linha em um .csv
    
    df %>%
       purrr::imap(
           ~ readr::write_csv(
               .x,
               file.path(source, "linhas", paste0(.y, ".csv")),
               append = TRUE
           )
       )
}
