# Given borough labelled data from 01_preprocess
# Estimate how many rides could have been shared/agregated
# Run this estimation as rolling window

# sql
library("RSQLite")
# data structure
library("data.table")
# par
library(doParallel)
# for dates
library('lubridate')
library("fasttime")
# for binning
library(ash)
# misc utils
library(pracma)

# function returns sql data from sql tables written in 01_preprocess
# all tables in given DB merged and returned as data.table
return_sql_as_dt <- function(drv, db_name){
  # given db driver and db name, returns data from sql table as DT
  con <- dbConnect(drv, dbname = db_name)
  # load and covert tables to r
  alltables = dbListTables(con)
  # load and merge all tables
  tbl_list = alltables
  # MM (Manhattan rides only) dataset too long to process for data challange
  # so split data in half, only load every other month
  # missing padding 0s, so have to hard code names
  if (strcmp(db_name, "MM.db")){
    #subset  
    tbl_list = c("trip_data_1",  "trip_data_3" , "trip_data_5" , 
                 "trip_data_7", "trip_data_9","trip_data_11") 
  }
  # again for MM data (2015 version) load every other month
  # the MM2015 dataset table are sortable by name, so no need to hardcode as above
  if (strcmp(db_name, "MM2015.db")){
    #subset  
    tbl_list = tbl_list[c(TRUE,FALSE)]
  }
  
  # for each table in DB
  a = data.table()
  for (lbl in tbl_list){
    print(lbl)
    r = data.table(dbGetQuery(con,paste('select * from',lbl) ))
    # convert time from number to datetime object
    r$pickup_datetime = as.POSIXct(r$pickup_datetime, origin="1970-01-01") # these could all be done in place, to re-optomize
    # convert to factors
    r$passenger_count = as.factor(r$passenger_count)
    r$pu_borough = as.factor(r$pu_borough)
    r$do_borough = as.factor(r$do_borough)
    a = rbind(a, r)
  }
  dbDisconnect(con)
  return(a)
}

# core algorithm
# given data.table of pickup and dropoff bin ids
# count up number in pickup
# count of number of unique locations for each pickup area
# estimate number of sharable rides
# return all measures
return_n_rides <- function(dt_in){
  min_cars = 0
  mytable <- xtabs(~do_id+pu_id, data=dt_in)
  num_pu = colSums(mytable)
  num_do = rowSums(mytable)
  u_dos = apply(mytable, 1, function(x) sum(x>0))
  # u_pus = apply(mytable, 2, function(x) sum(x>0))
  
  # this is counting regions with just one ride. 
  # will bias measure lower, wich is good, given 1 ride regions are not increasing efficiency
  # option to change is given by min_cars input
  num_with_same_do = (1.0 - (u_dos[num_do>min_cars] / num_do[num_do>min_cars])) * num_do[num_do>min_cars]
  raw_num_do = num_do[num_do>min_cars]
  uniq_do_areas = u_dos[num_do>min_cars]
  raw_num_pu = num_pu[num_do>min_cars]
  
  a = list(sum_cars_same_do = sum(num_with_same_do),
           raw_num_do = raw_num_do, 
           uniq_do_areas = uniq_do_areas, 
           num_with_same_do = num_with_same_do)
  return(a)
}


# start
drv <- dbDriver("SQLite")

# mnht 2 mnht trips, 2013
#   dbname = "MM.db"
# Brooklyn 2 Brooklyn trips, 2013
#   dbname = "BB2.db"
# Brooklyn 2 mnhtn trips, 2013
#   db_name = "BM.db"

# parallel used to apply shared ride estimator across time to datasets
cl <- makeCluster(4)
registerDoParallel(cl)


# hard code bin ranges, ensures they are consistant across 3 datasets
# these bin ranges go from top NW corner of MAN to SE of BROOK 
lo_range = c(-74.11558, -73.76037)
la_range = c(40.53121, 40.91864)
bns = 20
# Bin edges
e_lo <- seq(lo_range[1],lo_range[2],length=bns)	
e_la <- seq(la_range[1],la_range[2],length=bns)	

bin_lbls = cbind(e_lo, e_la)


# run loop for each DB name
# each DB name is a different ride dataset
#   BB: Brook - Brookm, MM: Manh - Manh, 
for (db_name in c("BM.db", "BB2.db", "BB2015.db","BM2015.db", "MM.db","MM2015.db")){
#for (db_name in c("MM.db","MM2015.db")){
    
    dat = return_sql_as_dt(drv,  db_name)
 
  #tic()
  # Pickup bins, for lat and long
  pu_lolo = histc(dat$pickup_longitude,e_lo)$bin
  pu_lala = histc(dat$pickup_latitude,e_la)$bin 
  # dropoff bins, for lat and long  
  do_lolo = histc(dat$dropoff_longitude,e_lo)$bin
  do_lala = histc(dat$dropoff_latitude,e_la)$bin 
  # convert to single id, one for pickup, one for dropoff  
  dat$pu_id = as.factor(pu_lolo + (pu_lala / 100))
  dat$do_id = as.factor(do_lolo + (do_lala / 100))
  #toc()
  
  #test = pu_lolo + (pu_lala / 100)
  #unique(BM$pu_id)
  #unique(BM$do_id)
  
  # set up win_starts for rolling window
  # window 15 minutes, step 5
  # win
  p_min = min(dat$pickup_datetime)
  p_max = max(dat$pickup_datetime)
  datetime <- seq(p_min,p_max, by = "5 min") 
  del = minutes(15)
  
  # for each window run algo
  # 365 * 24 * 60 / 5 = 105120 windows for one year
  
  x <- foreach(ii = 1:length(datetime), .packages=c('data.table','lubridate')) %dopar% {
    subset1 = dat[dat$pickup_datetime > datetime[ii] & 
                    dat$pickup_datetime < (datetime[ii] + del)]
    a = return_n_rides(subset1)$sum_cars_same_do
  }
  n_groupable_cars = (unlist(x))
  
  # n_groupable_cars as final measure
  # this is number of rides that could have been agregated

  out = data.table(datetime, n_groupable_cars)
  
  # write to db
  tn =  gsub(".db","", db_name)
  con <- dbConnect(drv, dbname = "rates_out_v1.db")
  dbWriteTable(con, tn, out, overwrite=TRUE)
  dbDisconnect(con)
}



