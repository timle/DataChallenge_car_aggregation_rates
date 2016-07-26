# it begins!


# /

# dat
library("data.table")
library("fasttime")
require("RSQLite")
library(rgdal)
library(rgeos) 
library(sp)
library(doParallel)

#base_folder = 'D:/dat/'
base_folder = 'C:/Users/tsk/Documents/2015_data/'
to_load = list.files(path = base_folder, pattern = '*.csv')


# first test, import data
# input = "D:/dat/trip_data_8.csv/trip_data_1.csv"
# dat = fread(input, header = TRUE, verbose = TRUE, drop = c(1,2,3,4,5),showProgress=TRUE)

files_n_load = 14:length(to_load)
files_n_load = 2:12
#files_n_load = 1:1

counties<-readOGR("nybb_16b/nybb.shp", layer="nybb")
counties = counties[counties$BoroName == 'Brooklyn' | counties$BoroName == 'Manhattan',]


library("rgdal")
library("rgeos")

map<-readOGR("test_shp", layer="nybb")

summary(map)




cl <- makeCluster(2)
registerDoParallel(cl)

l = data.table()
for (i in files_n_load){
  # first test, import data
  pth = paste0(base_folder,'/', to_load[i])
  
  print(pth)
  
  # for 2013 data
  #r_in = fread(pth, header = TRUE, verbose = TRUE, drop = c(1,2,3,4,5,7), showProgress=TRUE)
  
  #modifcations for 2015 data - works with yellow cab set
  # note, header order moves around for yellow vs green files
  # r_in = fread(pth, header = TRUE, verbose  = TRUE, 
  #             select =   c('tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 
  #                          'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude'), 
  #             showProgress=TRUE)
  #names(r_in)[names(r_in) == "tpep_pickup_datetime"] = "pickup_datetime" 
  #names(r_in)[names(r_in) == "tpep_dropoff_datetime"] = "dropoff_datetime" 
  
  # for green cab files
  r_in = fread(pth, header = TRUE, verbose  = TRUE, 
               select =   c('lpep_pickup_datetime', 'Lpep_dropoff_datetime', 'Passenger_count', 'Trip_distance', 'Pickup_longitude', 
                            'Pickup_latitude', 'Dropoff_longitude', 'Dropoff_latitude'), 
               showProgress=TRUE)
  
  names(r_in)[names(r_in) == "lpep_pickup_datetime"] = "pickup_datetime" 
  names(r_in)[names(r_in) == "Lpep_dropoff_datetime"] = "dropoff_datetime" 
  
  names(r_in)[names(r_in) == "Pickup_longitude"] = "pickup_longitude"
  names(r_in)[names(r_in) == "Pickup_latitude"] = "pickup_latitude"
  names(r_in)[names(r_in) == "Dropoff_longitude"] = "dropoff_longitude"
  names(r_in)[names(r_in) == "Dropoff_latitude"] = "dropoff_latitude"
  names(r_in)[names(r_in) == "Passenger_count"] = "passenger_count"
  names(r_in)[names(r_in) == "Trip_distance"] = "trip_distance"
  
  
  r_in <- r_in[, passenger_count := as.factor(passenger_count)]
  
  l = r_in
  
  # invalid lat or long coords on row
  rm = as.logical(abs(scale(l[,pickup_latitude])) > 1.5 | abs(scale(l[,pickup_longitude])) > 1.5 | abs(scale(l[,dropoff_latitude])) > 1.5 | abs(scale(l[,dropoff_longitude])) > 1.5)
  rm = rm | is.na(rm)
  mean(rm)
  dim(l)
  l <- l[!rm]
  dim(l)
  
  # other indicators of bad rows
  rm = l[,trip_distance]>1000 | l[,trip_distance]<=0
  dim(l)
  l <- l[!rm] 
  dim(l)
  
  # update datetimes, using fasttime library 
  l <- l[, pickup_datetime:=fastPOSIXct(pickup_datetime,'GMT')]
  # 
  
  
  #install.packages("maptools")
  #install.packages("rgeos")
  #library(maptools)
  #gor=readShapeSpatial("C:/Users/tsk/Downloads/nybb_16b/nybb",proj4string=CRS("+init=epsg:2263"))
  
  #man_shp = gor[gor$BoroName == 'Manhattan',]
  #brok_shp = gor[gor$BoroName == 'Brooklyn',]
  
  #plot(man_shp)
  
  #plot(brok_shp)
  
  
  
  # add simple boxes for manhattan>?
  
  
  
  
  
  
  # should be updated to re-project shape file instead of lat longs, but this works for now
  # prep cord data
  mapdata_pu = data.frame(l$pickup_longitude, l$pickup_latitude)
  colnames(mapdata_pu) <- c("longitude", "latitude")
  coordinates(mapdata_pu)<-~longitude+latitude
  proj4string(mapdata_pu)<-CRS("+proj=longlat +datum=NAD83")
  mapdata_pu<-spTransform(mapdata_pu, CRS(proj4string(counties)))
  
  mapdata_do = data.frame(l$dropoff_longitude, l$dropoff_latitude)
  colnames(mapdata_do) <- c("longitude", "latitude")
  coordinates(mapdata_do)<-~longitude+latitude
  proj4string(mapdata_do)<-CRS("+proj=longlat +datum=NAD83")
  mapdata_do<-spTransform(mapdata_do, CRS(proj4string(counties)))
  
  # obvi manhatan cords...
  # -74.01243 40.71625
  # -74.01243 40.75212
  # -73.97718 40.71625
  # -73.97718 40.75212
  
  
  
  # skeleton for parallel
  #l_len = dim(l)[1]
  #stepamnt = 10000
  #ss = seq(1,l_len,stepamnt)
  #ss = append(ss, l_len)
  #ss = ss[1:3]
  #result = list()
  #for (i in 1:(length(ss)-1)){
  #  s = ss[i]
  #  e = ss[i+1]-1
  #  result[i] = over(mapdata_do[s:e,], counties)[1] 
  #}
  
  
  # par process over function
  
  
  # par2
  ptm <- proc.time()
  # prep par
  l_len = dim(l)[1]
  stepamnt = 1000000
  ss = seq(1,l_len,stepamnt)
  ss = append(ss, (l_len+1))
  #do
  x <- foreach(a=ss[1:(length(ss)-1)], b=ss[2:(length(ss))]-1, .packages='sp') %dopar% {
    res = over(mapdata_do[a:b,], counties)[1]
  }
  s_do = unlist(x)
  s_do = unname(s_do)
  rm = is.na(s_do)
  #pu
  x <- foreach(a=ss[1:(length(ss)-1)], b=ss[2:(length(ss))]-1, .packages='sp') %dopar% {
    res = over(mapdata_pu[a:b,], counties)[1]
  }
  s_pu = unlist(x)
  s_pu = unname(s_pu)
  proc.time() - ptm
  
  # verifyy parallel code returned in correct order
  #slct =  sample(1:10000000, 100)
  #p = s_do[slct]
  #r = unname(unlist(over(mapdata_do[slct,], counties)[1]))
  #p == r
  
  l$pu_borough = s_pu
  l$do_borough = s_do
  dim(l)
  rm = is.na(s_pu) | is.na(s_do)
  l <- l[!rm,]
  dim(l)
  
  MM_li = l$pu_borough == 1 & l$do_borough == 1
  BB_li =  l$pu_borough == 3 & l$do_borough == 3
  BM_li =!MM_li & !BB_li
  
  #error check 
  #sum(MM_li) + sum(BB_li) + sum(BM_li)
  #dim(l)
  
  MM_trips = l[MM_li]
  BB_trips = l[BB_li]
  BM_trips = l[BM_li]
  dim(MM_trips)
  dim(BB_trips)
  dim(BM_trips)
  
  # write out?
  print('writing to sql')
  flush.console()
  
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





#library('ggplot2')
#ggplot() +  geom_polygon(data=counties, aes(x=long, y=lat, group=group))

#longitude = l[1:1000,]$pickup_longitude
#latitude = l[1:100,]$pickup_latitude
#mapdata_var = data.frame(longitude,latitude)
#ggplot() +  geom_point(data=mapdata_var, aes(x=longitude, y=latitude), color="red")

# translate to match shapefile projections
#coordinates(mapdata_var)<-~longitude+latitude
#proj4string(mapdata_var)<-CRS("+proj=longlat +datum=NAD83")
#mapdata_var<-spTransform(mapdata_var, CRS(proj4string(counties)))
#identical(proj4string(mapdata_var),proj4string(counties))

#mapdata_var_plot<-data.frame(mapdata_var)

#ggplot() + 
# geom_polygon(data=counties, aes(x=long, y=lat, group=group)) + 
#geom_point(data=mapdata_var_plot, aes(x=longitude, y=latitude), color="red")

# most of above from: http://zevross.com/blog/2014/07/16/mapping-in-r-using-the-ggplot2-package/

# overlap?






# 
# 
# # doing the over thing 
# #   http://gis.stackexchange.com/questions/133625/checking-if-points-fall-within-polygon-shapefile
# library(rgeos)
# library(sp)
# library(rgdal)
# 
# dat = mapdata_var
# 
# coordinates(dat) <- ~ longitude + latitude
# # Set the projection of the SpatialPointsDataFrame using the projection of the shapefile
# proj4string(dat) <- proj4string(counties)
# over(dat, counties)
# ### 
# 
# dat = over(mapdata_var, counties)[1]
# 
# # build boundaries
# rgn = 1:25000
# mapdata_pu = data.frame(l$pickup_longitude[rgn], l$pickup_latitude[rgn])
# colnames(mapdata_pu) <- c("longitude", "latitude")
# plot(x=mapdata_pu$longitude, y = mapdata_pu$latitude)
# 
# 
# 
# 
# 
# ## apply n/s boundaries, crude classification
# library(ggplot2)
# ## test boundaries
# 
# mapdata_pu = data.frame(l$pickup_longitude, l$pickup_latitude)
# colnames(mapdata_pu) <- c("longitude", "latitude")
# north_pu = (mapdata_pu$longitude < -74.00 & mapdata_pu$latitude > 40.70) | 
#         (mapdata_pu$longitude < -73.97 & mapdata_pu$latitude > 40.71) |
#         (mapdata_pu$longitude < -73 & mapdata_pu$latitude > 40.741)
# 
# mapdata_do = data.frame(l$dropoff_longitude, l$dropoff_latitude)
# colnames(mapdata_do) <- c("longitude", "latitude")
# north_do = (mapdata_do$longitude < -74.00 & mapdata_do$latitude > 40.70) | 
#   (mapdata_do$longitude < -73.97 & mapdata_do$latitude > 40.71) |
#   (mapdata_do$longitude < -73 & mapdata_do$latitude > 40.741)
# 
# 
# l$n_pu = north_pu
# l$n_do = north_do
# 
# nl = l[north_pu & north_do,]
# sl = l[!(north_pu & north_do),]
# summary(nl)
# summary(sl)
# summary(l)
# # boundaries dont seem right...
# # error check that. 
# 
# rgn = 1000000:1002000
# # for plotting
# # plot(x=mapdata_var2$longitude, y = mapdata_var2$latitude)
# mapdata_pl = data.frame(l$pickup_longitude[rgn], l$pickup_latitude[rgn])
# colnames(mapdata_pl) <- c("longitude", "latitude")
# # translate to match shapefile projections
# coordinates(mapdata_pl)<-~longitude+latitude
# proj4string(mapdata_pl)<-CRS("+proj=longlat +datum=NAD83")
# mapdata_pl<-spTransform(mapdata_pl, CRS(proj4string(counties)))
# 
# mapdata_do = data.frame(l$dropoff_longitude[rgn], l$dropoff_latitude[rgn])
# colnames(mapdata_do) <- c("longitude", "latitude")
# # translate to match shapefile projections
# coordinates(mapdata_do)<-~longitude+latitude
# proj4string(mapdata_do)<-CRS("+proj=longlat +datum=NAD83")
# mapdata_do<-spTransform(mapdata_do, CRS(proj4string(counties)))
# 
# # plot
# mapdata_pl_plot<-data.frame(mapdata_pl)
# mapdata_do_plot<-data.frame(mapdata_do)
# ggplot() + 
#   geom_polygon(data=counties, aes(x=long, y=lat, group=group)) + 
#   geom_point(data=mapdata_pl_plot, aes(x=longitude, y=latitude), color="red") + 
#   geom_point(data=mapdata_do_plot, aes(x=longitude, y=latitude), color="blue")
# 
# 
# ptm <- proc.time()
# 
# mapdata_pu = data.frame(sl$pickup_longitude, sl$pickup_latitude)
# colnames(mapdata_pu) <- c("longitude", "latitude")
# coordinates(mapdata_pu)<-~longitude+latitude
# proj4string(mapdata_pu)<-CRS("+proj=longlat +datum=NAD83")
# mapdata_pu<-spTransform(mapdata_pu, CRS(proj4string(counties)))
# pu_valid = !is.na((over(mapdata_pu, counties)[,2]))
# 
# mapdata_do = data.frame(sl$dropoff_longitude, sl$dropoff_latitude)
# colnames(mapdata_do) <- c("longitude", "latitude")
# coordinates(mapdata_do)<-~longitude+latitude
# proj4string(mapdata_do)<-CRS("+proj=longlat +datum=NAD83")
# mapdata_do<-spTransform(mapdata_do, CRS(proj4string(counties)))
# do_valid = !is.na((over(mapdata_do, counties)[,2]))
# 
# mean(pu_valid & do_valid)
# 
# proc.time() - ptm
# 
# 
# #mapdata_var_plot<-data.frame(mapdata_var2)
# 
# #coordinates(mapdata_var2) <- ~ longitude + latitude
# #proj4string(mapdata_var2) <- proj4string(counties)
# 
# 
# # plot
# mapdata_var_plot<-data.frame(mapdata_var2)
# ggplot() + 
#   geom_polygon(data=counties, aes(x=long, y=lat, group=group)) + 
#   geom_point(data=mapdata_var_plot[,], aes(x=longitude, y=latitude), color="red")
# 
# 

# possibly more efficient?
# http://stackoverflow.com/questions/21971447/check-if-point-is-in-spatial-object-which-consists-of-multiple-polygons-holes
