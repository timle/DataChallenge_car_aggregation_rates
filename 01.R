# it begins!


# 

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


# update datetimes, using fasttime library 
l <- l[, pickup_datetime:=fastPOSIXct(pickup_datetime,'GMT')]


# remove bad rows
rm = l[,trip_distance]>1000 | l[,trip_distance]<=0
dim(l)
l <- l[!rm] 
dim(l)


install.packages("maptools")
install.packages("rgeos")

library(maptools)


gor=readShapeSpatial("C:/Users/tsk/Downloads/nybb_16b/nybb",proj4string=CRS("+init=epsg:2263"))


man_shp = gor[gor$BoroName == 'Manhattan',]
brok_shp = gor[gor$BoroName == 'Brooklyn',]

plot(man_shp)

plot(brok_shp)


library(rgdal)
counties<-readOGR("C:/Users/tsk/Downloads/nybb_16b/nybb.shp", layer="nybb")

ggplot() +  geom_polygon(data=counties, aes(x=long, y=lat, group=group))

longitude = l[1:100,]$pickup_longitude
latitude = l[1:100,]$pickup_latitude

rm = abs(scale(longitude)) > 2 | abs(scale(latitude)) > 2
mean(rm)
mapdata_var = data.frame(longitude,latitude)
mapdata_var = mapdata_var[!rm,]
ggplot() +  geom_point(data=mapdata_var, aes(x=longitude, y=latitude), color="red")

# translate to match shapefile projections
coordinates(mapdata_var)<-~longitude+latitude
proj4string(mapdata_var)<-CRS("+proj=longlat +datum=NAD83")
mapdata_var<-spTransform(mapdata_var, CRS(proj4string(counties)))
identical(proj4string(mapdata_var),proj4string(counties))
mapdata_var<-data.frame(mapdata_var)

ggplot() + 
geom_polygon(data=counties, aes(x=long, y=lat, group=group)) + 
geom_point(data=mapdata_var, aes(x=longitude, y=latitude), color="red")

# most of above from: http://zevross.com/blog/2014/07/16/mapping-in-r-using-the-ggplot2-package/



# doing the over thing 
#   http://gis.stackexchange.com/questions/133625/checking-if-points-fall-within-polygon-shapefile
library(rgeos)
library(sp)
library(rgdal)

over(mapdata_var, counties)

dat = mapdata_var

coordinates(dat) <- ~ longitude + latitude
# Set the projection of the SpatialPointsDataFrame using the projection of the shapefile
proj4string(dat) <- proj4string(counties)
over(dat, counties)
### 


## combine the two top bits?



#ansform(lnd, CRS("+init=epsg:4326")) # reproject

library("ggplot2")
lnd_f <- fortify(man_shp) # you may need to load maptools
lnd_f[1:10,]


golibrary("rjson")
json_file <- "http://nyctaximap.appspot.com/data/nta.json"
jdat = readLines(json_file)
json_data <- fromJSON(paste(readLines(json_file), collapse=""))


