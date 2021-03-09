The wget command will download a file from a link you provide. Using --output-document will let you specify where you want the file to be downloaded. The API call requires an API key along with a date and will return all delisted stock up to that date (March 5th 2021, in this example).

`wget --output-document "fullDelisted.csv" "https://www.alphavantage.co/query?function=LISTING_STATUS&date=2021-03-05&state=delisted&apikey=YourAPIKey"`


Sending the contents of the newly created file to sed "1d" removes the first row (the column headings) from the file. Passing the remaining data to awk allows you to specify a delimiter and then use only the first piece of the line based on that delimiter. In this case the first piece is the stock symbol which is sent to a new file.

`cat fullDelisted.csv | sed "1d" | awk -F ',' '{print $1}' > 'delisted.csv'`
