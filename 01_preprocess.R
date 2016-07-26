# Preprocess taxi data, save as sqlite files
#
# Given source folder of csv files, remove and clean data
# Identify boroughs for pickup and dropoff
# Save out Manhattan - Manhattan, Brooklyn - Brooklyn, and Manhattan - Brooklyn
# All data saved as sqlite tables, naming based on source csv file

# dat
library("data.table")
library("fasttime")
library("RSQLite")
library(doParallel)
# Mapping
library(rgdal)
library(rgeos) 
library(sp)


#base_folder contains NYC Taxi csv files
base_folder = '/2015_data/'
to_load = list.files(path = base_folder, pattern = '*.csv')

# work on all files in folder
files_n_load = 1:length(to_load)
#work on subset of files in folder
#files_n_load = 2:12
#files_n_load = 1:1

#load shapefiles for NYC boroughs
# from: http://www1.nyc.gov/site/planning/data-maps/open-data/districts-download-metadata.page
counties<-readOGR("nybb_16b/nybb.shp", layer="nybb")
# restrict to just Manhattan and Brooklyn
counties = counties[counties$BoroName == 'Brooklyn' | counties$BoroName == 'Manhattan',]

#summary(counties)

# for parallel computing, set up number of nodes before entering loop:
# Will use parallel for borough identification
# this processes, of identifying which points fall within which 
# boroughs is quite computationally intensive
cl <- makeCluster(2)
registerDoParallel(cl)

# Will run this loop for each file
# 1) loads csv into memory
# 2) removes rows that have outliers for pickup and dropoff points
# 3) idenitfy pickup and dropoff points in Manhattan, Brooklyn, or other
#       This is done in parallel
# 4) organize into Brooklyn/Brooklyn, Manhattan/Brooklyn, Manhattan/Manhattan 
#       Datasets
# 5) Save out to Sql

# TODO: clean up this code, convert to functions
l = data.table()
for (i in files_n_load){
  # build path, to load file
  pth = paste0(base_folder,'/', to_load[i])
  print(pth)
  
  # given year of dataset, there are different ways to handle the files
  # this is not yet automated, and requires manipulating the script 
  #  depending on which year, and which type, of data is being loaded
  
  ## for 2013 data  - simplest
  r_in = fread(pth, header = TRUE, verbose = TRUE, drop = c(1,2,3,4,5,7), showProgress=TRUE)
  
  ## for 2015 data - yellow cab set
  # note, header order moves around for yellow vs green files
  # r_in = fread(pth, header = TRUE, verbose  = TRUE, 
  #             select =   c('tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 
  #                          'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude'), 
  #             showProgress=TRUE)
  #names(r_in)[names(r_in) == "tpep_pickup_datetime"] = "pickup_datetime" 
  #names(r_in)[names(r_in) == "tpep_dropoff_datetime"] = "dropoff_datetime" 
  
  ## for green cab files
  #r_in = fread(pth, header = TRUE, verbose  = TRUE, 
  #             select =   c('lpep_pickup_datetime', 'Lpep_dropoff_datetime', 'Passenger_count', 'Trip_distance', 'Pickup_longitude', 
  #                          'Pickup_latitude', 'Dropoff_longitude', 'Dropoff_latitude'), 
  #             showProgress=TRUE)
  
  # rename columns, if 2015 yellow cab data
  names(r_in)[names(r_in) == "lpep_pickup_datetime"] = "pickup_datetime" 
  names(r_in)[names(r_in) == "Lpep_dropoff_datetime"] = "dropoff_datetime" 
  # rename columns, if 2015 green cab data
  names(r_in)[names(r_in) == "Pickup_longitude"] = "pickup_longitude"
  names(r_in)[names(r_in) == "Pickup_latitude"] = "pickup_latitude"
  names(r_in)[names(r_in) == "Dropoff_longitude"] = "dropoff_longitude"
  names(r_in)[names(r_in) == "Dropoff_latitude"] = "dropoff_latitude"
  names(r_in)[names(r_in) == "Passenger_count"] = "passenger_count"
  names(r_in)[names(r_in) == "Trip_distance"] = "trip_distance"
  
  # to factor, save space
  r_in <- r_in[, passenger_count := as.factor(passenger_count)]
  
  # rename (depreciated, can be removed)
  l = r_in
  
  # remove invalid lat or long coords on row
  # considering points with z score > 1.5 as outliers
  # based on identifying threshold of likely outliers by plotting points
  # most points concentrated around NYC, so lower z score (1.5) is effective at
  # getting the outlier points
  # helpful to remove these now, saves having to do shapefile identification
  # which is quite processor intensive
  rm = as.logical(abs(scale(l[,pickup_latitude])) > 1.5 | abs(scale(l[,pickup_longitude])) > 1.5 | abs(scale(l[,dropoff_latitude])) > 1.5 | abs(scale(l[,dropoff_longitude])) > 1.5)
  rm = rm | is.na(rm)
  sprintf('removing %f percent of data, due to outliers',  mean(rm))
  #dim(l)
  l <- l[!rm]
  #dim(l)
  
  # other indicators of bad rows
  # high trip distance, low trip distance
  rm = l[,trip_distance]>1000 | l[,trip_distance]<=0
  #dim(l)
  l <- l[!rm] 
  #dim(l)
  
  # convert char datetimes to r datetime object using fasttime library 
  l <- l[, pickup_datetime:=fastPOSIXct(pickup_datetime,'GMT')]
  

  # Code should be updated to re-project shape file instead of 
  # reprojecting lat longs of pickup and dropoffs 
  # but this approach works for now
  # prep cord data
  # reproject pickup points to match coordinates in NYC shapefile
  mapdata_pu = data.frame(l$pickup_longitude, l$pickup_latitude)
  colnames(mapdata_pu) <- c("longitude", "latitude")
  coordinates(mapdata_pu)<-~longitude+latitude
  proj4string(mapdata_pu)<-CRS("+proj=longlat +datum=NAD83")
  mapdata_pu<-spTransform(mapdata_pu, CRS(proj4string(counties)))
  # reproject dropoff points to match coordinates in NYC shapefile
  mapdata_do = data.frame(l$dropoff_longitude, l$dropoff_latitude)
  colnames(mapdata_do) <- c("longitude", "latitude")
  coordinates(mapdata_do)<-~longitude+latitude
  proj4string(mapdata_do)<-CRS("+proj=longlat +datum=NAD83")
  mapdata_do<-spTransform(mapdata_do, CRS(proj4string(counties)))

  
  ## identifying borough for each pickup and dropoff point
  #   Thisis quite cpu intensive
  #     will divide into jobs, and distribute across processors. 
  
  # ptm <- proc.time() # for timing
  
  # create start and end points for dataset division
  l_len = dim(l)[1]
  stepamnt = 1000000
  ss = seq(1,l_len,stepamnt)
  ss = append(ss, (l_len+1))
  # do parallel for drop off points
  # ss maps the data into chunks according to stepamnt
  x <- foreach(a=ss[1:(length(ss)-1)], b=ss[2:(length(ss))]-1, .packages='sp') %dopar% {
    res = over(mapdata_do[a:b,], counties)[1]
  }
  s_do = unlist(x)
  s_do = unname(s_do)
  #rm = is.na(s_do)
  # do parallel for drop off points
  x <- foreach(a=ss[1:(length(ss)-1)], b=ss[2:(length(ss))]-1, .packages='sp') %dopar% {
    res = over(mapdata_pu[a:b,], counties)[1]
  }
  s_pu = unlist(x)
  s_pu = unname(s_pu)
  # proc.time() - ptm # for timing
  
  # verify parallel code returned in correct order
  #slct =  sample(1:10000000, 100)
  #p = s_do[slct]
  #r = unname(unlist(over(mapdata_do[slct,], counties)[1]))
  #p == r
  
  # back into data.table
  l$pu_borough = s_pu
  l$do_borough = s_do
  #dim(l)
  # remove points if pickup or dropoff isn't in brooklyn or manhattan
  rm = is.na(s_pu) | is.na(s_do)
  l <- l[!rm,]
  #dim(l)
  
  # identify manhattan - manhattan, brooklyn - brooklyn, brooklyn - manhattan, trips
  MM_li = l$pu_borough == 1 & l$do_borough == 1
  BB_li =  l$pu_borough == 3 & l$do_borough == 3
  BM_li =!MM_li & !BB_li
  
  #error check 
  #sum(MM_li) + sum(BB_li) + sum(BM_li)
  #dim(l)
  
  # subset the data
  MM_trips = l[MM_li]
  BB_trips = l[BB_li]
  BM_trips = l[BM_li]
  #dim(MM_trips)
  #dim(BB_trips)
  #dim(BM_trips)

  # write out
  # will use csv as table name, db name as datatype
  print('writing to sql')
  #flush.console()
  
  tn =  gsub(".csv|-", "", to_load[i])
  fn= 'test_file_out.csv'
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = "MM2015.db")
  dbWriteTable(con, tn, MM_trips,overwrite=TRUE)
  dbDisconnect(con)
  
  con <- dbConnect(drv, dbname = "BB2015.db")
  dbWriteTable(con, tn, BB_trips,overwrite=TRUE)
  dbDisconnect(con)
  
  con <- dbConnect(drv, dbname = "BM2015.db")
  dbWriteTable(con, tn, BM_trips,overwrite=TRUE)
  dbDisconnect(con)

}

