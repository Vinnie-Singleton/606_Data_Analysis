# 606_Data_Analysis

This repo contains code for Hadoop, Spark, and Unix pertaining to course [606 Big Data Management](https://www.stonehill.edu/programs/data-analytics-masters/courses/). The data that is examined as part of the course focuses on NYC Taxi rides pulled from [AWS](https://registry.opendata.aws/nyc-tlc-trip-records-pds/) and on stock market data gathered from [Alpha Vantage](https://www.alphavantage.co/).

## Getting Started
To learn more about Alpha Vantage and get your API Key (which is necessary to make queries) you can visit their website and review their [documentation](https://www.alphavantage.co/documentation/) for accessing data.

The [NYC Taxi data](https://registry.opendata.aws/nyc-tlc-trip-records-pds/) is freely available from AWS and only requires you to [sign up](https://aws.amazon.com/free/) with a free account.

## Usage
This code is meant for reference for Unix commands and for filtering and manipulating data in Spark and Hadoop.

Each folder, NYC Taxi and Stock Market Data, holds a collection of files related to accessing and manipulating data using Unix commands and Spark/Hadoop code. For example, to get a list of delisted stock you could reference the Stock Market Data folder and find the following piece of code which uses Alpha Vantage's API along with some Unix commands to gather and manipulate the data into a usable file called delisted.csv.

### Stock Market Data > Getting Delisted Stocks:

`wget --output-document "fullDelisted.csv" "https://www.alphavantage.co/query?function=LISTING_STATUS&date=2021-03-05&state=delisted&apikey=YourAPIKey"`

`cat fullDelisted.csv | sed "1d" | awk -F ',' '{print $1}' > 'delisted.csv'`






