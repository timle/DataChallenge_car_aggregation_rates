# first round data look


require("RSQLite")
require('data.table')
require('lubridate')
require('pracma')
library('ggplot2')
library(scales)


tp = data.frame(lbl = c('BB_2013','BB_2015','BM_2013','BM_2015'),
                db_name = c("BB2","BB2015","BM","BM2015"))


for (r in 1:dim(tp)[1]){
  r = tp[r,]
  plt_months(r$lbl, r$db_name)
}




for (r in 1:dim(tp)[1]){
  r = tp[r,]

  to_all = list(wkd_lim = c(0,24), 
            sa_lim = c(0,24), 
            su_lim = c(0,24))
  
  to_via = list(wkd_lim = c(6,24), 
                sa_lim = c(10,24), 
                su_lim = c(10,21))
  
  to_cust = list(wkd_lim = c(12,20), 
                sa_lim = c(12,24), 
                su_lim = c(12,20))
  
  
  numas_all = n_cars(r$lbl, r$db_name, FALSE, to_all)
  print(sprintf('all_hours, %s, %f r/h',r$lbl,numas_all$rides_hour))
  
  numas_cust = n_cars(r$lbl, r$db_name, FALSE, to_cust)
  # 121
  print(sprintf('custom, %s, %f r/h',r$lbl,numas_cust$rides_hour))
  
  # to_via: 130.9 rides an hour
  numas_via = n_cars(r$lbl, r$db_name, FALSE, to_via)
  # # 119.9
  print(sprintf('Via, %s, %f r/h',r$lbl,numas_via$rides_hour))
  
}



n_cars <- function(data_lbl, db_name, do_plot, to){
  # give counts for # similar rides, by day x month
  
  wkd_lim = to$wkd_lim
  sa_lim = to$sa_lim
  su_lim = to$su_lim
  
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = "rates_out_v1.db")
  #alltables = dbListTables(con)
  
  rate_dat = data.table(dbGetQuery(con, sprintf('select * from %s',db_name) ))
  rate_dat$datetime = as.POSIXct(rate_dat$datetime, origin="1970-01-01")
  rate_dat$day_id = day(rate_dat$datetime)
  rate_dat$month_id = as.factor(month(rate_dat$datetime))
  wkd = wday(rate_dat$datetime) # (0-6 starting on Sunday).
  lbls = c('Sun','Mon','Tue','Wed','Thur','Fri','Sat')
  rate_dat$weekdays =lbls[wkd]
  rate_dat$hour = hour(rate_dat$datetime) # 
  rate_dat$minute = minute(rate_dat$datetime) 
  
  subs = rate_dat[c(TRUE,FALSE,FALSE),]
  
  is_sat = subs$weekdays == "Sat"
  is_sun = subs$weekdays == "Sun"
  is_wkd = !(is_sat | is_sun)
  
  # sat filter
  sat_li = is_sat & subs$hour >= sa_lim[1] & subs$hour <= sa_lim[2]
  
  # sun filter
  sun_li = is_sun & subs$hour >= su_lim[1] & subs$hour <= su_lim[2]
  
  # wkd filter
  wkd_li = is_wkd & subs$hour >= wkd_lim[1] & subs$hour <= wkd_lim[2]
  
  hr_li = sat_li | sun_li | wkd_li
  #print(dim(subs))
  subs = subs[hr_li,]
  #print(sprintf('mean hrs used form set %f',mean(hr_li)))
  #print(dim(subs))
  
  n_out = list()
  n_out$minutes_in_set = as.numeric(sum(diff(subs$datetime)))
  n_out$hours_in_set = n_out$minutes_in_set / 60
  n_out$rides_in_set = sum(subs$n_groupable_cars)
  n_out$rides_hour = n_out$rides_in_set/n_out$hours_in_set
  
  
  day_sums = subs[, sum(n_groupable_cars), 
                       by=list(month_id, weekdays, day_id)]
  
  day_sums_mnthly_avg = day_sums[, mean(V1), 
                  by=list(month_id, weekdays)]
  
  day_sums_mnthly_avg$weekdays = as.factor(day_sums_mnthly_avg$weekdays)
  day_sums_mnthly_avg$weekdays <- factor(day_sums_mnthly_avg$weekdays, levels = lbls)
  
  if (do_plot){
    p <- ggplot(day_sums_mnthly_avg, 
                aes(x=weekdays, y=V1, 
                    group = month_id,
                    fill = month_id))
    
    lim_txt = sprintf('Sat %i-%i, Sun %i-%i, Wkdy %i-%i', 
                      sa_lim[1], sa_lim[2],
                      su_lim[1], su_lim[2],
                      wkd_lim[1], wkd_lim[2])
    
    
    p + geom_bar(stat="identity",position="dodge") + 
      theme(text = element_text(size=20),
            axis.text.x = element_text(angle = 45, hjust = 1)) +
      ggtitle(sprintf('%s - n Similiar Rides\nbetween %s',data_lbl,lim_txt)) + 
      coord_cartesian(ylim = c(0, 20000)) + 
      xlab('Day x Month') + 
      ylab('n Similiar Rides')
    
    fn = sprintf("daily_counts__hrs-%s__%s.png",data_lbl,lim_txt)
    mypath <- file.path("figs",fn)
    
    ggsave(mypath, plot = last_plot(), scale = 1, dpi = 300)
    print(mypath)
  }
  
  return(n_out)
}


plt_months <- function(data_lbl, db_name){

  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = "rates_out_v1.db")
  #alltables = dbListTables(con)
  
  rate_dat = data.table(dbGetQuery(con, sprintf('select * from %s',db_name) ))
  rate_dat$datetime = as.POSIXct(rate_dat$datetime, origin="1970-01-01")
  # add day and time info to cols
  rate_dat$day_id = day(rate_dat$datetime)
  rate_dat$month_id = month(rate_dat$datetime)
  wkd = wday(rate_dat$datetime) # (0-6 starting on Sunday).
  lbls = c('Sun','Mon','Tue','Wed','Thur','Fri','Sat')
  rate_dat$weekdays =lbls[wkd]
  rate_dat$hour = hour(rate_dat$datetime) # 
  rate_dat$minute = minute(rate_dat$datetime) 

  # mean for each day in month
  day_means = rate_dat[, n_similar_rides := mean(n_groupable_cars), 
                       by=list(month_id, weekdays, hour, minute)]
  day_means$dt = as.POSIXct(make_datetime(hour = day_means$hour, 
                               min = day_means$minute, 
                               year = 2013, month = day_means$month_id))
  
  # reorder for legend
  day_means$weekdays = as.factor(day_means$weekdays)
  day_means$weekdays <- factor(day_means$weekdays, levels = lbls)
  
  data_id = data_lbl
  
  r_max = max(day_means$n_similar_rides)
  for (i in 1:12) {
    mypath <- file.path("figs",paste(data_id, "__", i, ".png", sep = ""))
    
    # one month
    temp = day_means[day_means$month_id == i]
    p <- ggplot(temp, 
                aes(x=dt, y=n_similar_rides, 
                    group = weekdays,
                    colour = weekdays))
    p + geom_line(size=1.25) + 
      theme(text = element_text(size=20),
            axis.text.x = element_text(angle = 45, hjust = 1)) +
    ggtitle(sprintf('Month %i \n Mean Similiar rides, by day of week',i)) + 
      coord_cartesian(ylim = c(0, r_max)) + 
      scale_x_datetime(date_labels = "%H:%M", breaks = date_breaks("2 hours"))+
      xlab('Time of Day') + 
      ylab('n Similar Rides')
    
    ggsave(mypath, plot = last_plot(), scale = 1)
    print(mypath)
  }
}


       
       








