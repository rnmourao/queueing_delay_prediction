# limpa area de trabalho do R
rm(list = ls())

# funcao que calcula a diferenca em segundos de dois horarios
diferenca_em_segundos <- function(hora_depois, hora_antes="00:00:00") {
  antes <- as.numeric(substr(hora_antes, start=1, stop=2)) * 3600 +
    as.numeric(substr(hora_antes, start=4, stop=5)) * 60 +
    as.numeric(substr(hora_antes, start=7, stop=8))
  
  depois <- as.numeric(substr(hora_depois, start=1, stop=2)) * 3600 +
    as.numeric(substr(hora_depois, start=4, stop=5)) * 60 +
    as.numeric(substr(hora_depois, start=7, stop=8))
  
  return (depois - antes)
}

# funcao para normalizar campos numericos
normalizar <- function(x) if (min(x) == max(x)) 0.5 else (x-min(x))/(max(x)-min(x))

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

# todos as datas coletadas
these_days <- sort(as.character(unique(services$date)))

# remove variavel inutil
services$dif_bsb <- NULL

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

## calcular tempos

services$arrival_time <- as.character(services$arrival_time)
services$start_time <- as.character(services$start_time)
services$end_time <- as.character(services$end_time)
services$reneging_time <- as.character(services$reneging_time)

services$end_time[services$status == "reneged"] <- services$reneging_time[services$status == "reneged"]
services$reneging_time <- NULL

# waiting_time
services$waiting_time <- diferenca_em_segundos(services$start_time, services$arrival_time)

# service_time
services$service_time <- diferenca_em_segundos(services$end_time, services$start_time)

# para facilitar os calculos
services$arrival_time_secs <- diferenca_em_segundos(hora_depois = services$arrival_time)
services$start_time_secs <- diferenca_em_segundos(hora_depois = services$start_time)


# remove services com waiting_time menor que zero. Nao ha como obter informacoes relevantes nessa situacao,
# pois o cliente nao foi ao menos chamado.
services <- subset(services, services$waiting_time >= 0)

# substitui por zero service_time negativos.
services$service_time[services$service_time < 0] <- 0

# mudar todos com status started para reneged.
services$status[services$status == 'started'] <- 'reneged'

# agora so existem atendimentos finalizados ou que desistiram.

# time_overflow <- alvo
services$time_overflow <- FALSE
services$time_overflow[services$waiting_time >= 1800] <- TRUE

## calcula quantos atendimentos ocorreram no dia anterior

history <- aggregate(x = services$sequence, by = list(date = services$date, branch_id = services$branch_id), FUN=length)
names(history)[3] <- 'count'
history <- history[order(history$branch_id, history$date),]

today <- sort(as.character(unique(history$date)))
tomorrow <- sort(tail(today, -1))
tomorrow <- union(tomorrow, c(NA))
day_n_tomorrow <- data.frame(date = as.Date(today), yesterday = as.Date(today), tomorrow = as.Date(tomorrow))

history <- merge(x = history, y = day_n_tomorrow, all.x = TRUE)
history$date <- NULL
names(history) <- c("branch_id", "yesterday_count", "yesterday", "date")

services <- merge(services, history, all.x = TRUE)

# remove NAs substituindo por media do yesterday_count do branch no mes
for (i in 1:nrow(services)) {
  services$yesterday_count[i] <- trunc(mean(history$yesterday_count[history$branch_id == services$branch_id[i]]))
}


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

# ordena atendimentos
services <- services[order(services$date, 
                           services$branch_id, 
                           services$queue_id, 
                           services$sequence),]

# monta lista de filas por dia
queues_per_day <- unique(data.frame(date = services$date,
                                    branch_id = services$branch_id,
                                    queue_id = services$queue_id))


# inicializa novas colunas
services$queue_length <- 0
services$clients_count <- 0
services$priorities_count <- 0
services$companies_count <- 0
services$avg_service_time <- 0
services$interarrival_time <- 0
services$arrival_rate <- 0
services$service_rate <- 0
services$utilization_factor <- 0
services$hol <- 0
services$rcs <- 0
services$les <- 0
services$tol <- 0
services$peak_hour <- FALSE

# criacao das novas colunas
for (i in 1:nrow(services)) {
   
   # clients_today: todos os clientes que foram atendidos no mesmo dia e mesma fila que eu.
   services_today <- services[services$date == services$date[i] &
                               services$branch_id == services$branch_id[i] &
                               services$queue_id == services$queue_id[i],]

   # interarrival_time: quanto tempo depois que eu cheguei em relacao ao que chegou imediatamente
   #                    antes de mim
   if (services$sequence[i] != services_today$sequence[1]) {
      services$interarrival_time[i] <- services$arrival_time_secs[i] - 
                                       services$arrival_time_secs[i - 1]
   }
   
   # before: pessoas que chegaram antes de mim e ja foram chamadas
   before <- services[services$date == services$date[i] &
                        services$branch_id == services$branch_id[i] &
                        services$queue_id == services$queue_id[i] &
                        services$sequence < services$sequence[i] &
                        services$start_time_secs < services$arrival_time_secs[i],]

   # served: pessoas que chegaram antes de mim e ja foram atendidas   
   served <- services[before$sequence < services$sequence[i] & before$end_time < services$arrival_time[i],]

   # not_served: pessoas que chegaram antes de mim e ainda nao foram atendidas   
   not_served <- services[before$sequence < services$sequence[i] & before$end_time > services$arrival_time[i],]
      
   # RCS: tempo de espera do ultimo cliente que foi atendido antes de eu chegar.
   if (nrow(served) != 0) {
     services$rcs[i] <- served$waiting_time[nrow(served)]
   }

   # LES: tempo de espera do ultimo cliente que chegou antes de mim e ainda nao terminou
   # de ser atendido.
   if (nrow(not_served) != 0) {
     services$les[i] <- not_served$waiting_time[nrow(not_served)]
   }
      
   

   # queue: as pessoas que estao na minha frente sao aquelas que chegaram antes de mim, i.e.,
   #        sequence menor que o meu e que ainda nao foram atendidas no momento que eu cheguei
   queue <- services[services$date == services$date[i] &
                       services$branch_id == services$branch_id[i] &
                       services$queue_id == services$queue_id[i] &
                       services$sequence < services$sequence[i] &
                       services$start_time_secs > services$arrival_time_secs[i],]

   # tamanho da fila  
   services$queue_length[i] <- nrow(queue)
   
   # quantidade de correntistas na fila
   services$clients_count[i] <- nrow(queue[!is.na(queue$client_id),])
   
   # quantidade de prioritarios na fila
   services$priorities_count[i] <- nrow(queue[queue$priority == TRUE,])
   
   # quantidade de empresas na fila
   services$companies_count[i] <- nrow(queue[queue$entity == 'company',])
   
   # tempo medio de atendimento da fila (dos que ja foram atendidos)
   if (nrow(before) > 0) {
      services$avg_service_time[i] <- mean(before$service_time)
   }
   
   # HOL: tempo de espera de quem encabeca a fila
   # TOL: tail-of-line - o tempo de espera do cliente que esta na minha frente
   if (nrow(queue) != 0) {
     services$hol[i] <- services$arrival_time_secs[i] - queue$arrival_time_secs[1]
     services$tol[i] <- services$arrival_time_secs[i] - queue$arrival_time_secs[nrow(queue)]
   } 
   
   if (nrow(queue) > 0) {
      interarrival_avg <- mean(queue$interarrival_time)
   } else {
      interarrival_avg <- 0
   }

   # taxa de chegada
   if (interarrival_avg != 0) {
     services$arrival_rate[i] <- 1 / interarrival_avg
   }

   # taxa de servico
   if (services$avg_service_time[i] != 0) {
      services$service_rate[i] <- 1 / services$avg_service_time[i]
   }

   # fator de utilizacao
   if (services$service_rate[i] != 0) {
      services$utilization_factor[i] <- services$arrival_rate[i] / services$service_rate[i]
   }
   
   # identificar se o cliente chegou em um horario de pico de ontem
   if (!is.na(services$yesterday[i])) {
     services_yesterday <- services[services$date == services$yesterday[i] &
                                      services$branch_id == services$branch_id[i] &
                                      services$queue_id == services$queue_id[i],]
     
     # utiliza a funcao hist para obter os intervalos e as frequencias.
     # depois verifica se o horario do atendimento atual esta na faixa de horarios de pico,
     # que defini como a partir do Q3 (terceiro quartil).
     # entao, a quantidade de pessoas que chegaram em algum horario ultrapassou 75% da quantidade
     # do dia anterior, esse horario eh definido como de pico.
     if (nrow(services_yesterday) > 0) {
       h <- hist(services_yesterday$arrival_time_secs, plot = FALSE, breaks = 10)
       q3 <- quantile(h$counts, names = FALSE)[4]
       for (j in 1:length(h$counts)) {
         if (h$counts[j] > q3) {
           if (services$arrival_time_secs[i] >= h$breaks[j] & 
               services$arrival_time_secs[i] <= h$breaks[j + 1]) {
             services$peak_hour[i] <- TRUE
           }
         }        
       }
     }
   }
}

# quantidade de nao correntistas
services$non_clients_count <- services$queue_length - services$clients_count

# quantidade de nao prioritarios
services$non_priorities_count <- services$queue_length - services$priorities_count

# quantidade de pessoas fisicas
services$non_companies_count <- services$queue_length - services$companies_count

# criar campo que unifica branch_id e queue_id. O ultimo se repete entre os branches,
# nao gerando informacao relevante separado do primeiro
services$branch_n_queue <- paste(services$branch_id, "_", services$queue_id, sep = "")

# removendo outliers
services <- services[services$interarrival_time >= 0,]

## separar services_fit em treinamento (60%), validacao (20%) e teste (20%)
set.seed(1980)
services$set <- sample.int(n = 3, size = nrow(services), replace = TRUE, prob = c(.6, .2, .2))

## agora tem que preparar o data frame de análise, tirando os campos desnecessarios
services_fit <- services[,c(13, 16, 19, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 18, 39)]

services_training <- services_fit[services_fit$set == 1,]
services_validation <- services_fit[services_fit$set == 2,]
services_test <- services_fit[services_fit$set == 3,]

# fazer undersampling pois a classe time_overflow = TRUE eh rara (< 20%) e quero privilegia-la.
services_training_TRUE <- services_training[services_training$time_overflow == TRUE,]
services_training_FALSE <- services_training[services_training$time_overflow == FALSE,]
services_training_FALSE <- services_training_FALSE[sample(nrow(services_training_FALSE), nrow(services_training_TRUE)),]
services_training <- rbind(services_training_FALSE, services_training_TRUE)

# junta tudo de novo
services_fit <- rbind(services_training, services_validation, services_test)

# grava csvs
write.csv(services_fit, file = "/home/publico/mestrado/mineracao/raw/gat/services_fit.csv", row.names = FALSE)
write.csv(services, file = "/home/publico/mestrado/mineracao/raw/gat/services_full.csv", row.names = FALSE)
