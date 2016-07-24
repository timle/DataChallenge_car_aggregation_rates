# load, first analysis
library("RSQLite")
library("data.table")
library("fasttime")


return_sql_as_dt <- function(drv, db_name){
  # given db driver and db name, returns data from sql table as DT
  con <- dbConnect(drv, dbname = db_name)
  # load and covert tables to r
  alltables = dbListTables(con)
  # load and merge all tables
  tbl_list = alltables
  a = data.table()
  for (lbl in tbl_list){
    print(lbl)
    r = data.table(dbGetQuery(con,paste('select * from',lbl) ))
    a = rbind(a, r)
  }
  # conversions back into r
  # date
  a$pickup_datetime = as.POSIXct(a$pickup_datetime, origin="1970-01-01")
  #check for dropoff_datetime...
  # convert if present
  # factors
  a$passenger_count = as.factor(a$passenger_count)
  a$pu_borough = as.factor(a$pu_borough)
  a$do_borough = as.factor(a$do_borough)
  dbDisconnect(con)
  return(a)
}


drv <- dbDriver("SQLite")

# mnht 2 mnht trips, 2013
dbname = "MM.db"

# Brooklyn 2 Brooklyn trips, 2013
dbname = "BB2.db"

# Brooklyn 2 mnhtn trips, 2013
db_name = "BM.db"


# BM
BM = return_sql_as_dt(drv,  "BM.db")

# BB
BB = return_sql_as_dt(drv,  "BB2.db")

# MM
MM = return_sql_as_dt(drv,  "MM.db")





r =   sample(1:dim(BB)[1], 100)
plot.new()



p_la = BB$pickup_latitude
p_lo = BB$pickup_longitude
d_la = BB$dropoff_latitude
d_lo = BB$dropoff_longitude

library('ggplot2')

# 1000 random ggplot points
r =   sample(1:dim(BB)[1], 1000)
t = BB[r,]
ggplot() + 
  geom_segment(aes(y = pickup_latitude, x = pickup_longitude, 
                   yend = dropoff_latitude, xend = dropoff_longitude, 
                   colour = "segment"
  ), 
  alpha = .15,
  data = t) + 
  geom_point(aes(y = pickup_latitude, x = pickup_longitude
  ),
  shape = 'd', alpha = .15, color = 'green',
  data = t) + 
  geom_point(aes(y = dropoff_latitude, x = dropoff_longitude
  ),
  shape = 'd', alpha = .15, color = 'blue',
  data = t) + 
  geom_polygon(data=map_wgs84, aes(x=long, y=lat, group=group)) 




plot.new()
s = 1
plot(p_la[s], p_lo[s], main = "arrows(.) and segments(.)")
arrows(p_la[s], p_lo[s], d_la[s], d_lo[s],length = 0.25)


#lines(x= c(BB$pickup_longitude[r],BB$pickup_latitude[r]), y = c(BB$dropoff_longitude[r],BB$dropoff_latitude[r]) )

lines(x = c(0,0), y = c(2,3))
BB[1:10]

# load borough data, reproject to wsg84
# map<-readOGR("C:/Users/tsk/Downloads/nybb_16b/nybb.shp", layer="nybb")
# map_wgs84 <- spTransform(map, CRS("+proj=longlat +datum=WGS84"))
# plot(map_wgs84, axes=TRUE)
# plot on top
# ggplot() + 
#   geom_polygon(data=map_wgs84, aes(x=long, y=lat, group=group)) + 
#   geom_point(data=t, aes(x=pickup_longitude, y=pickup_latitude), color="red")

# http://www.r-bloggers.com/5-ways-to-do-2d-histograms-in-r/

# distances between all points. how fast is it?
# http://www.inside-r.org/packages/cran/raster/docs/pointDistance
