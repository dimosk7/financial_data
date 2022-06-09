# Indexing

To retrieve data more quickly, we create indexes on tables ```daily_prices``` and ```fin_stat```. In the section below, we test query performance for table ```fin_stat```.

~~~ mysql

SET profiling = 1;

select  
from fin_stat
where Ticker = PBIP;

CREATE INDEX index_f
ON fin_stat(Ticker, id);

select  
from fin_stat
where Ticker = PBIP;

show profiles;

~~~

Results shown on the image below 

![Alt text](/tables_samples/indexing.PNG?raw=true Optional Title)
