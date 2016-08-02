Code for Data Challange, assesing car volume, in NYC, using the New York City taxi data set:
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

This code computes the number of rides that could have been shared, given a time window, and list of rides for a given area. The output can be used for time series analysis. 

If you are interested in my findings, please get in touch. I am unable to post the results publicly as they are under consideration, as part of a data challange performed for a private company. 


There are three scripts in the pipeline:

## 01_preprocess.R
Loads Taxi data csvs, cleans, idenitfies boroughs, and saves out sqllite files. 

## 02_predict_rides.R
Using custom metric, identifies number of rides that could have been shared, given a dataset of ride information.

## 03_rates_analysis.R
Given results from 02_predict_rides, generates plots and summary statistics for reporting. 

## nybb_16b/
Contains shape files used for identifying NYC borough boundaries.

