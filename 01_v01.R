# it begins!


# /

# dat
library("data.table")
library("fasttime")


base_folder = 'D:/dat/trip_data'
to_load = list.files(path = base_folder, pattern = '*.csv')


# first test, import data
# input = "D:/dat/trip_data_8.csv/trip_data_1.csv"
# dat = fread(input, header = TRUE, verbose = TRUE, drop = c(1,2,3,4,5),showProgress=TRUE)

#files_n_load = 1:length(to_load)
files_n_load = 1:1

l = data.table()
for (i in files_n_load){
  # first test, import data
  pth = paste0(base_folder,'/', to_load[i])
  
  print(pth)
  
  r_in = fread(pth, header = TRUE, verbose = TRUE, drop = c(1,2,3,4,5,7), showProgress=TRUE)
  
  r_in <- r_in[, passenger_count := as.factor(passenger_count)]
  
  l = rbind(l, r_in)
}


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



#install.packages("maptools")
#install.packages("rgeos")
#library(maptools)
#gor=readShapeSpatial("C:/Users/tsk/Downloads/nybb_16b/nybb",proj4string=CRS("+init=epsg:2263"))

#man_shp = gor[gor$BoroName == 'Manhattan',]
#brok_shp = gor[gor$BoroName == 'Brooklyn',]

#plot(man_shp)

#plot(brok_shp)

library(rgdal)
counties<-readOGR("C:/Users/tsk/Downloads/nybb_16b/nybb.shp", layer="nybb")

counties = counties[counties$BoroName == 'Brooklyn',]




library('ggplot2')
#ggplot() +  geom_polygon(data=counties, aes(x=long, y=lat, group=group))

longitude = l[1:1000,]$pickup_longitude
latitude = l[1:100,]$pickup_latitude
#mapdata_var = data.frame(longitude,latitude)
#ggplot() +  geom_point(data=mapdata_var, aes(x=longitude, y=latitude), color="red")

# translate to match shapefile projections
coordinates(mapdata_var)<-~longitude+latitude
proj4string(mapdata_var)<-CRS("+proj=longlat +datum=NAD83")
mapdata_var<-spTransform(mapdata_var, CRS(proj4string(counties)))
identical(proj4string(mapdata_var),proj4string(counties))

mapdata_var_plot<-data.frame(mapdata_var)

ggplot() + 
  geom_polygon(data=counties, aes(x=long, y=lat, group=group)) + 
  geom_point(data=mapdata_var_plot, aes(x=longitude, y=latitude), color="red")

# most of above from: http://zevross.com/blog/2014/07/16/mapping-in-r-using-the-ggplot2-package/

# overlap?
library(rgeos) 





# doing the over thing 
#   http://gis.stackexchange.com/questions/133625/checking-if-points-fall-within-polygon-shapefile
library(rgeos)
library(sp)
library(rgdal)

dat = mapdata_var

coordinates(dat) <- ~ longitude + latitude
# Set the projection of the SpatialPointsDataFrame using the projection of the shapefile
proj4string(dat) <- proj4string(counties)
over(dat, counties)
### 

dat = over(mapdata_var, counties)[1]

# build boundaries
rgn = 1:25000
mapdata_pl = data.frame(l$pickup_longitude[rgn], l$pickup_latitude[rgn])
colnames(mapdata_pu) <- c("longitude", "latitude")
plot(x=mapdata_pu$longitude, y = mapdata_pu$latitude)





## apply n/s boundaries, crude classification
library(ggplot2)
## test boundaries

mapdata_pu = data.frame(l$pickup_longitude, l$pickup_latitude)
colnames(mapdata_pu) <- c("longitude", "latitude")
north_pu = (mapdata_pu$longitude < -74.00 & mapdata_pu$latitude > 40.70) | 
        (mapdata_pu$longitude < -73.97 & mapdata_pu$latitude > 40.71) |
        (mapdata_pu$longitude < -73 & mapdata_pu$latitude > 40.741)

mapdata_do = data.frame(l$dropoff_longitude, l$dropoff_latitude)
colnames(mapdata_do) <- c("longitude", "latitude")
north_do = (mapdata_do$longitude < -74.00 & mapdata_do$latitude > 40.70) | 
  (mapdata_do$longitude < -73.97 & mapdata_do$latitude > 40.71) |
  (mapdata_do$longitude < -73 & mapdata_do$latitude > 40.741)


l$n_pu = north_pu
l$n_do = north_do

nl = l[north_pu & north_do,]
sl = l[!(north_pu & north_do),]
summary(nl)
summary(sl)
summary(l)
# boundaries dont seem right...
# error check that. 

rgn = 1000000:1002000
# for plotting
# plot(x=mapdata_var2$longitude, y = mapdata_var2$latitude)
mapdata_pl = data.frame(l$pickup_longitude[rgn], l$pickup_latitude[rgn])
colnames(mapdata_pl) <- c("longitude", "latitude")
# translate to match shapefile projections
coordinates(mapdata_pl)<-~longitude+latitude
proj4string(mapdata_pl)<-CRS("+proj=longlat +datum=NAD83")
mapdata_pl<-spTransform(mapdata_pl, CRS(proj4string(counties)))

mapdata_do = data.frame(l$dropoff_longitude[rgn], l$dropoff_latitude[rgn])
colnames(mapdata_do) <- c("longitude", "latitude")
# translate to match shapefile projections
coordinates(mapdata_do)<-~longitude+latitude
proj4string(mapdata_do)<-CRS("+proj=longlat +datum=NAD83")
mapdata_do<-spTransform(mapdata_do, CRS(proj4string(counties)))

# plot
mapdata_pl_plot<-data.frame(mapdata_pl)
mapdata_do_plot<-data.frame(mapdata_do)
ggplot() + 
  geom_polygon(data=counties, aes(x=long, y=lat, group=group)) + 
  geom_point(data=mapdata_pl_plot, aes(x=longitude, y=latitude), color="red") + 
  geom_point(data=mapdata_do_plot, aes(x=longitude, y=latitude), color="blue")


ptm <- proc.time()

mapdata_pu = data.frame(sl$pickup_longitude, sl$pickup_latitude)
colnames(mapdata_pu) <- c("longitude", "latitude")
coordinates(mapdata_pu)<-~longitude+latitude
proj4string(mapdata_pu)<-CRS("+proj=longlat +datum=NAD83")
mapdata_pu<-spTransform(mapdata_pu, CRS(proj4string(counties)))
pu_valid = !is.na((over(mapdata_pu, counties)[,2]))

mapdata_do = data.frame(sl$dropoff_longitude, sl$dropoff_latitude)
colnames(mapdata_do) <- c("longitude", "latitude")
coordinates(mapdata_do)<-~longitude+latitude
proj4string(mapdata_do)<-CRS("+proj=longlat +datum=NAD83")
mapdata_do<-spTransform(mapdata_do, CRS(proj4string(counties)))
do_valid = !is.na((over(mapdata_do, counties)[,2]))

mean(pu_valid & do_valid)

proc.time() - ptm


#mapdata_var_plot<-data.frame(mapdata_var2)

#coordinates(mapdata_var2) <- ~ longitude + latitude
#proj4string(mapdata_var2) <- proj4string(counties)


# plot
mapdata_var_plot<-data.frame(mapdata_var2)
ggplot() + 
  geom_polygon(data=counties, aes(x=long, y=lat, group=group)) + 
  geom_point(data=mapdata_var_plot[,], aes(x=longitude, y=latitude), color="red")



# possibly more efficient?
# http://stackoverflow.com/questions/21971447/check-if-point-is-in-spatial-object-which-consists-of-multiple-polygons-holes
