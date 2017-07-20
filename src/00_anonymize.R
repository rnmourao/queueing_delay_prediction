###########################################################################
# Esse codigo nao deve ser executado, pois foi utilizado para mascarar os #
# dados. O produto desse codigo eh o arquivo services.csv, insumo para os # 
# outros scripts.                                                         #
###########################################################################

# limpa area de trabalho do R
rm(list = ls())

## inicio da preparacao de dados

arquivos = c(160801, 160802, 160803, 160804, 160805, 160808, 160809, 160810, 160811, 160812, 160815, 160816, 160817, 160818, 160819, 160822, 160823, 160824, 160825, 160826, 160829, 160830, 160831)

# unifica arquivos diarios
services <- NULL
for (i in 1:length(arquivos)) {
  temp <- read.csv(paste("/home/f8676628/ws_kdz/local/zOSsrc/arquivos/D", arquivos[i], ".csv", sep = ""), header=FALSE, sep=";")
  temp$dt <- as.Date(paste("20", arquivos[i], sep = ""), format = "%Y%m%d")
  
  if (is.null(services)) {
    services <- temp
  } else {
    services <- rbind(services, temp)
  }
}

# muda nomes das colunas
names(services) <- c("dependencia", "queue_id", "sequence", "mci", "funcionario", "status", "entity", "priority", "arrival_time", "start_time", "end_time", "reneging_time", "servico", "dif_bsb", "date")

## mascarar dados e substituir codigos

# dependencias
depes <- sort(unique(services$dependencia))
tabela_depes <- data.frame(branch_id = seq(1:length(depes)), dependencia = depes)
services <- merge(tabela_depes, services)
services$dependencia <- NULL

# mci
clientes <- sort(unique(services$mci))
tabela_clientes <- data.frame(client_id = seq(1:length(clientes)), mci = clientes)
tabela_clientes$client_id[tabela_clientes$mci == 0] <- NA
services <- merge(services, tabela_clientes)
services$mci <- NULL

# funcionario
funcionarios <- sort(unique(services$funcionario))
tabela_funcionarios <-  data.frame(cashier = seq(1:length(funcionarios)), funcionario = funcionarios)
tabela_funcionarios$cashier[1:2] <- NA
services <- merge(services, tabela_funcionarios)
services$funcionario <- NULL

# servico
x <- sort(unique(services$servico))
y <- head(LETTERS, length(x))
tabela_servicos <- data.frame(servico = x, service_type = y)
services <- merge(services, tabela_servicos)
services$servico <- NULL

# status
services$status[services$status == 10] <- "waiting"
services$status[services$status == 20] <- "started"
services$status[services$status == 30] <- "reneged"
services$status[services$status == 40] <- "finished"
services$status[services$status == 15] <- "finished"
services$status <- as.factor(services$status)

services$end_time[services$status == "reneged"] <- services$reneging_time[services$status == "reneged"]
services$reneging_time <- NULL

# mudar todos com status started para reneged.
services$status[services$status == 'started'] <- 'reneged'
# agora so existem atendimentos finalizados ou que desistiram.

# entity
services$entity <- as.character(services$entity)
services$entity[services$entity == "0"] <- NA
services$entity[services$entity == "1"] <- "person"
services$entity[services$entity == "2"] <- "company"
services$entity <- as.factor(services$entity)

# priority
services$priority <- as.factor(services$priority)
levels(services$priority)[levels(services$priority) != "0"] <- "TRUE"
levels(services$priority)[levels(services$priority) == "0"] <- "FALSE"

# remove variavel inutil
services$dif_bsb <- NULL

# retirar filas que tiveram mais de um atendente.
cashiers <- unique(data.frame(date = services$date,
                              branch_id = services$branch_id, 
                              queue_id = services$queue_id, 
                              cashier = services$cashier))


cashiers <- aggregate(x=cashiers$cashier, by = list(date = cashiers$date,
                                                    branch_id = cashiers$branch_id,
                                                    queue_id = cashiers$queue_id), FUN=length)

cashiers <- cashiers[cashiers$x == 1,]
cashiers$x <- NULL

services <- merge(services, cashiers)
# agora so existem filas que tem um caixa.

write.csv(services, file = "/home/mourao/queueing_delay_prediction/services.csv", row.names = FALSE)
