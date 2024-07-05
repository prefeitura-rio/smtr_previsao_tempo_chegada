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
    
    df <- bigrquery::bq_table_download(
        table
    )
    
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

# arquivo full

query <- readr::read_file("projecao.sql")

q <- query %>%
    gsub("\\{start_date\\}", "\"2024-05-01\"", .) %>%
    gsub("\\{end_date\\}", "\"2024-05-31\"", .)

bigrquery::bq_auth(
    email = "igor.rilave@hotmail.com"
)

table <- bigrquery::bq_project_query(
    "absolute-text-417919",
    query = q
)

df <- bigrquery::bq_table_download(
    table
)

readr::write_csv(df, file.path(source, "base_validacao.csv"))
