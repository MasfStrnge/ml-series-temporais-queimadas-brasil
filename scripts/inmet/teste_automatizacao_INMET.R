library(RSelenium)

# força usar geckodriver (Firefox)
rD <- rsDriver(browser = "firefox", port = 4545L, 
               chromever = NULL, 
               phantomver = NULL)

remDr <- rD$client
remDr$navigate("https://bdmep.inmet.gov.br/")


# localizar o <a> pelo seletor de classe
link <- remDr$findElement(using = "css selector", value = "a.instrucoes_proximo.submit")

# clicar no link
link$clickElement()



# preencher campo de e-mail
webElem <- remDr$findElement(using = "name", value = "email")
webElem$sendKeysToElement(list("manuvanish@gmail.com"))

# clicar no botão
btn <- remDr$findElement(using = "xpath", value = "//button[@type='submit']")
btn$clickElement()


