# 606_Data_Analysis

This repo contains code for Hadoop and Spark pertaining to course 606-Distributed-Computing.

# Getting Started
To learn more about Alpha Vantage and get your API Key (which is necessary to make queries) you can visit their [website](https://www.alphavantage.co/documentation/)

# Usage
This code is meant for reference for unix commands and code relating to  filtering and manipulating data in spark and Hadoop. The data referenced relates to NYC Taxi data pulled from Amazon S3 and Alpha Vantage.

## Getting Delisted Stocks:

`wget --output-document "fullDelisted.csv" "https://www.alphavantage.co/query?function=LISTING_STATUS&date=2021-03-05&state=delisted&apikey=YourAPIKey"`

`cat fullDelisted.csv | sed "1d" | awk -F ',' '{print $1}' > 'delisted.csv'`






