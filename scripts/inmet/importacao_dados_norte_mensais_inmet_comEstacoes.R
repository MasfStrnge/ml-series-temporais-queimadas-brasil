#######################################################################
#       IMPORTAÇÃO DOS DADOS MENSAIS DO INMET NO BANCO DUCKDB 
#######################################################################

# Pacotes
library(DBI)
library(duckdb)
library(dplyr)
library(lubridate)

# Conexão com o banco
con <- dbConnect(duckdb(), dbdir = "database/banco_de_dados_IC.duckdb", read_only = FALSE)

# Caminho da pasta com os CSVs
caminho_pasta <- "data/INMET_dados_norte_mensaiss/"  

# Lista de arquivos CSV
arquivos_csv <- list.files(path = caminho_pasta, pattern = "\\.csv$", full.names = TRUE)

# Função auxiliar para converter vírgula em ponto e transformar em número
corrige_num <- function(x) {
  x[x %in% c("null", "")] <- NA   # trata valores "null" como NA
  as.numeric(gsub(",", ".", x))
}

# Loop para importar cada arquivo
for (arquivo in arquivos_csv) {
  dados <- read.csv2(
    arquivo,
    sep = ";",
    header = TRUE,
    skip = 10,              # pula as 10 primeiras linhas de metadados
    stringsAsFactors = FALSE
  )
  
  cat("Arquivo:", arquivo, "lido\n\n")
  
  # Remove última coluna vazia, se existir
  if (all(is.na(dados[[ncol(dados)]])) || all(dados[[ncol(dados)]] == "")) { 
    dados <- dados[, -ncol(dados)] 
  } 
  
  # Padroniza nomes das colunas
  colnames(dados) <- c(
    "data_medicao",
    "dias_chuva_mensal",
    "precip_total_mensal_mm",
    "temp_media_mensal_c"
  )
  
  # Corrige vírgula decimal e valores "null"
  dados$dias_chuva_mensal      <- corrige_num(dados$dias_chuva_mensal)
  dados$precip_total_mensal_mm <- corrige_num(dados$precip_total_mensal_mm)
  dados$temp_media_mensal_c    <- corrige_num(dados$temp_media_mensal_c)
  
  # Converte a coluna de data
  dados$data_medicao <- as.Date(dados$data_medicao, format = "%Y-%m-%d")
  
  # Adiciona código da estação a partir do nome do arquivo
  dados$codigo_estacao <- gsub(".*_(A\\d{3})_.*", "\\1", basename(arquivo))
  
  # Grava no banco (append = TRUE para acumular todos os arquivos)
  dbWriteTable(con, "inmet_dados_norte_mensal_teste", dados, append = TRUE)
}

# Finaliza conexão
dbDisconnect(con)

