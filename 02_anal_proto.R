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





# binnings?






library(ash)
library(pracma)

test = 
BM$pickup_datetime[1]


bns = 20
lo_range = sort(range(cbind(BM$pickup_longitude, BM$dropoff_longitude)))
lo_range = lo_range + c(-(abs(lo_range[1]))* .001, abs(lo_range[2] * .001))
la_range = sort(range(cbind(BM$pickup_latitude, BM$dropoff_latitude)))
la_range = la_range + c(-(abs(la_range[1]))* .001, abs(la_range[2] * .001))

e_lo <- seq(lo_range[1],lo_range[2],length=bns)	
e_la <- seq(la_range[1],la_range[2],length=bns)	

tic()
pu_lolo = histc(BM$pickup_longitude,e_lo)$bin
pu_lala = histc(BM$pickup_latitude,e_la)$bin 

do_lolo = histc(BM$dropoff_longitude,e_lo)$bin
do_lala = histc(BM$dropoff_latitude,e_la)$bin 

BM$pu_id = as.factor(pu_lolo + (pu_lala / 100))
BM$do_id = as.factor(do_lolo + (do_lala / 100))
toc()

test = pu_lolo + (pu_lala / 100)
unique(BM$pu_id)
unique(BM$do_id)


barplot(table(BM$do_id),horiz = TRUE)

bin_lbls = cbind(e_lo, e_la)

# distribution of pickup rides by location. 
# 2d histo would be beter...
#library('ggplot2')
#c <- ggplot(BM, aes(do_id))
#c + geom_bar()+ coord_flip()


library(lubridate)

datetime <- seq(min(BM$pickup_datetime), max(BM$pickup_datetime), by = "1 hour")    
datetime2 <- seq(min(BM$pickup_datetime) + minutes(30), max(BM$pickup_datetime), by = "1 hour")    

datetime[1:10]
datetime2[1:10]

# first hour of trips:
del = minutes(15)
li = BM$pickup_datetime > datetime[1] & BM$pickup_datetime < datetime[1] + del
bm_ts = BM[li]

mytable <- xtabs(~do_id+pu_id, data=bm_ts)
num_pu = colSums(mytable)
num_do = rowSums(mytable)
u_dos = apply(mytable, 1, function(x) sum(x>0))
# u_pus = apply(mytable, 2, function(x) sum(x>0))




# this is counting regions with just one ride. 
# will bias measure lower, wich is good, given 1 ride regions are not increasing efficiency

num_with_same_do = (1.0 - (u_dos[num_do>0] / num_do[num_do>0])) * num_do[num_do>0]
raw_num_do = num_do[num_do>0]
uniq_do_areas = u_dos[num_do>0]
raw_num_pu = num_pu[num_do>0]

data.frame(raw_num_do, uniq_do_areas, num_with_same_do)





mytable <- xtabs(~do_id+pu_id, data=BM)
mytable =  mytable[order(rownames(mytable)),order(colnames(mytable))] 


#normalized for total rides
mytable2 <- sweep(mytable,2,colSums(mytable),`/`)

# for each pickup location, what is max dropoff number:
m = apply(mytable, 2, max)
barplot(m)

# for each pickup location, what is max dropoff number:
m = apply(mytable2, 2, max)
barplot(m)

cli = colSums(mytable) > 10000
m = apply(mytable2[,cli], 2, max)
barplot(m)

cli = colSums(mytable) > 10000
m = apply(mytable2[,cli], 2, median)
barplot(m[m>0])

cli = colSums(mytable) > 10000
m = apply(mytable2[,cli], 2, sd)
barplot(m)


dev.off()
par(mfrow=c(4,1)) 
cli = colSums(mytable) > 100000
m = apply(mytable2[,cli], 2, max)
barplot(m,main = 'max')

cli = colSums(mytable) > 100000
m = apply(mytable2[,cli], 2, median)
barplot(m, main = 'median')

cli = colSums(mytable) > 100000
m = apply(mytable2[,cli], 2, mean)
barplot(m, main = 'sd')

#cli = colSums(mytable) > 10000
#m = apply(mytable2[,cli], 2, function(x) sum(x>(1/(99*99))) )
#barplot(m, main = 'sd')

dev.off()
par(mfrow=c(4,1)) 
cli = colSums(mytable) > 100000
m = apply(mytable2[,cli], 2, function(x) quantile(x,.95))
barplot(m, main = 'sd')

cli = colSums(mytable) > 100000
m = apply(mytable2[,cli], 2, function(x) quantile(x,.75))
barplot(m, main = 'sd')

cli = colSums(mytable) > 100000
m = apply(mytable2[,cli], 2, function(x) quantile(x,.4))
barplot(m, main = 'sd')

cli = colSums(mytable) > 100000
m = apply(mytable2[,cli], 2, function(x) quantile(x,.05))
barplot(m, main = 'sd')




library(corrplot)
rli = rowSums(mytable) > 10000
cli = colSums(mytable) > 10000
image(log(mytable[rli,cli]+1),col = heat.colors(100))

image((mytable2[rli,cli]),col = heat.colors(100))


# for each pickup location, what is max norm dropoff number:
m = apply(mytable2, 2, max)
cs = colSums(mytable)
barplot(m[cs>1000])

# for each pickup location, what is sd dropoff number:
m = apply(mytable2, 2, sd)
barplot(m)

# median
m = apply(mytable2, 2, median)
barplot(m)

#sum sanity check
m = apply(mytable2, 2, sum)
barplot(m)

# need sanity check...


BM_t[ , `:=`( COUNT = .N , IDX = 1:.N ) , by = VAL ]


li = test = '4.05'




# verify
# ab <- matrix( c(0,0,12,12), 2, 2) # interval [-5,5) x [-5,5)
# bins <- bin2(cbind(x,y), ab, c(12,12)) # bin counts,ab,nskip

      

#x <- matrix( rnorm(200), 100 , 2) # bivariate normal n=100
ab <- matrix( c(-5,-5,5,5), 2, 2) # interval [-5,5) x [-5,5)
nbin <- c( 10, 10) # 400 bins
bins <- bin2(dat, , nbin) # bin counts,ab,nskip




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
