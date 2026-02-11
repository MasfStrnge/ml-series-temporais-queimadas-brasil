###############################################################################
###### ETL DE DADOS DO SIH-RD (SISTEMA DE INTERNAÇÕES HOSPITALARES) ###########
###############################################################################


# INSTALANDO PACOTES
install.packages("remotes") # pacote para acessar reposit??rio remoto
install.packages("duckdb")  # pacote do banco de dados DuckDB
install.packages("DBI")     # Interface para banco de dados do R
install.packages("duckplyr")


# Pacote do microdatasus 
remotes::install_github("rfsaldanha/microdatasus")


# CARREGANDO PACOTES
library(microdatasus)
library(DBI)
library(duckdb)
library(duckplyr)

# Conectando o DuckDB
con <- dbConnect(duckdb(), dbdir = "database/banco_de_dados_IC.duckdb", read_only = FALSE)


# Selecionando as variáveis
vars_select = c("N_AIH", "ANO_CMPT", "MES_CMPT", "UF_ZI", "MUNIC_RES", "NASC", "IDADE", "SEXO", 
                "DIAG_PRINC", "QT_DIARIAS", "DT_INTER", "DT_SAIDA",  "VAL_SP", "VAL_TOT")

# Selecionando as cids_select
cids_select <- c("J40", "J41", "J42", "J43", "J44", "J45", "J46",
                 "J00", "J01", "J02", "J03", "J04", "J05", "J06", "J09",
                 "J10", "J11", "J12","J13","J14","J15","J16","J17", "J18",
                 "J20", "J21", "J22")

  
# Importanto o ano de 2015 no banco de dados
  cat("Importando ano:", 2015, "\n")
  fetch_datasus(
    year_start = 2015, year_end = 2015,
    month_start = 1, month_end = 12,
    uf = c("AC", "AP", "AM", "PA", "RO", "RR", "TO"),
    #vars = vars_select,
    information_system = "SIH-RD"
  ) %>%
    filter(DIAG_PRINC %in% cids_select) %>% { dbWriteTable(con, "sih_dados_completo",., append = TRUE) } 
  cat("Finished")
  
  
# Um for loop para baixar do ano 2016 até o ano de 2024  
  for (x in 2016 : 2024) {
  cat("Importando ano:", x, "\n")
  dados <- fetch_datasus(
    year_start = x, year_end = x,
    month_start = 1, month_end = 12,
    uf = c("AC", "AP", "AM", "PA", "RO", "RR", "TO"),
    vars = vars_select,
    information_system = "SIH-RD"
    ) %>%
     filter(DIAG_PRINC %in% cids_select) %>%
    { dbWriteTable(con, "sih_dados_teste", ., append = TRUE) }
  cat("\n", x, "terminado\n")
}
 
 # finalizando a conexão com o Duckdb
  dbDisconnect(con)
  