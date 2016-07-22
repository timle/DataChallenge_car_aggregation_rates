# load, first analysis
library("RSQLite")
library("data.table")
library("fasttime")

drv <- dbDriver("SQLite")

# mnht 2 mnht trips
con <- dbConnect(drv, dbname = "MM.db")
dbDisconnect(con)


# Brooklyn 2 Brooklyn trips
con <- dbConnect(drv, dbname = "BB2.db")
dbDisconnect(con)


# Brooklyn 2 mnht trips
con <- dbConnect(drv, dbname = "BM.db")
dbDisconnect(con)




alltables = dbListTables(con)

tbl_list = alltables
a = data.table()
for (lbl in tbl_list){
  
  r = data.table(dbGetQuery(con,paste('select * from',lbl) ))
  a = rbind(a, r)
}

p1 = data.table(dbGetQuery(con,'select * from trip_data_1' ))
p2 = data.table(dbGetQuery(con,'select * from trip_data_2' ))


# conversions back into r
# date
p1$pickup_datetime = as.POSIXct(p1$pickup_datetime, origin="1970-01-01")
# factors
p1$passenger_count = as.factor(p1$passenger_count)
p1$pu_borough = as.factor(p1$pu_borough)
p1$do_borough = as.factor(p1$do_borough)


