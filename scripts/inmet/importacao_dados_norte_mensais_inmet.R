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

# Junta todos os arquivos em um único data.frame
lista_dados <- lapply(arquivos_csv, function(arquivo) {
  dados <- read.csv2(
    arquivo,
    sep = ";",
    header = TRUE,
    skip = 10,
    stringsAsFactors = FALSE
  )
  
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
  
  # Corrige vírgula decimal
  dados$dias_chuva_mensal      <- corrige_num(dados$dias_chuva_mensal)
  dados$precip_total_mensal_mm <- corrige_num(dados$precip_total_mensal_mm)
  dados$temp_media_mensal_c    <- corrige_num(dados$temp_media_mensal_c)
  
  # Converte a coluna de data
  dados$data_medicao <- as.Date(dados$data_medicao, format = "%Y-%m-%d")
  
  # Cria colunas ano e mes
  dados$ano <- ifelse(is.na(dados$data_medicao), NA, as.integer(format(dados$data_medicao, "%Y")))
  dados$mes <- ifelse(is.na(dados$data_medicao), NA, as.integer(format(dados$data_medicao, "%m")))
  
  # Remove a coluna data_medicao
  dados <- dados %>% select(-data_medicao)
  
  return(dados)
})

# Combina todos os arquivos
dados_final <- bind_rows(lista_dados)

# Consolida por ano/mês calculando médias
dados_final <- dados_final %>%
  group_by(ano, mes) %>%
  summarise(
    dias_chuva_mensal      = mean(dias_chuva_mensal, na.rm = TRUE),
    precip_total_mensal_mm = mean(precip_total_mensal_mm, na.rm = TRUE),
    temp_media_mensal_c    = mean(temp_media_mensal_c, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(ano, mes)

# Grava no banco (sobrescreve para garantir estrutura correta)
dbWriteTable(con, "inmet_dados_norte_mensal_teste4", dados_final, overwrite = TRUE)

# Finaliza conexão
dbDisconnect(con)


