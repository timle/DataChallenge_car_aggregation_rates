# first round data look


require("RSQLite")
require('data.table')
require('lubridate')
require('pracma')
library('ggplot2')

drv <- dbDriver("SQLite")
con <- dbConnect(drv, dbname = "rates_out_v1.db")
alltables = dbListTables(con)

BB_13 = data.table(dbGetQuery(con, ('select * from BB2') ))
BB_13$datetime = as.POSIXct(BB_13$datetime, origin="1970-01-01")

BB_13$day_id = day(BB_13$datetime)
BB_13$month_id = month(BB_13$datetime)
wkd = wday(BB_13$datetime) # (0-6 starting on Sunday).
lbls = c('Sun','Mon','Tue','Wed','Thur','Fri','Sat')
BB_13$weekdays =lbls[wkd]

BB_13$hour = hour(BB_13$datetime) # 
BB_13$minute = minute(BB_13$datetime) 

#time(BB_13$datetime)
# date range squence?
#temp = BB_13[BB_13$day_id == 1 & BB_13$month_id == 2]
# plot(temp$datetime,temp$n_groupable_cars)

# mean for each day
new = BB_13[, n_similar_rides := mean(n_groupable_cars), by=list(month_id, weekdays, hour, minute)]
new$dt = make_datetime(hour = new$hour, min = new$minute, year = 2013, month = new$month_id)
# reorder
new$weekdays = as.factor(new$weekdays)
levels(new$weekdays)
new$weekdays <- factor(new$weekdays, levels = lbls)

data_id = 'BB_13'

r_max = max(new$n_similar_rides)
for (i in 1:12) {
  mypath <- file.path("figs",paste(data_id, "__", i, ".png", sep = ""))
  
  # one month
  temp = new[new$month_id == i]
  p <- ggplot(temp, 
              aes(x=dt, y=n_similar_rides, 
                  group = weekdays,
                  colour = weekdays))
  p + geom_line(size=1.25) + 
    theme(text = element_text(size=20),
          axis.text.x = element_text()) +
  ggtitle(sprintf('Month %i',i)) + 
    coord_cartesian(ylim = c(0, r_max)) 
  
  ggsave(mypath, plot = last_plot(), scale = 1)
  print(mypath)
}

       
       








