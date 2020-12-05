This project can download finviz and yahoo finance stock and options data and save to either a database (stockarchivedb.py) or csv (stockarhivefile.py).

If saving to csvs
# Create a data folder and subfolders
`mkdir -p data/finviz data/yf/daily data/yf/intraday data/yf/options`

If saving to a database

```
psql -U postgres
to login as local postgres admin

postgres=# create database mydb;
postgres=# create user myuser with encrypted password 'mypass';
postgres=# grant all privileges on database mydb to myuser;
```

Save password in .pgpass file
Update model.py connection string

Copy the github folder
Install dependencies
Update cronjob

Yahoo API rate limits
2000 requests/hour/IP address
https://developer.yahoo.com/yql/guide/overview.html#usage-information-and-limits

