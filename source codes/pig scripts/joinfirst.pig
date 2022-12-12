DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

Checkout = LOAD 's3://data/Checkouts_by_Title.csv' USING CSVLoader(',') as (UsageClass: chararray, CheckoutType: chararray, MaterialType: chararray, CheckoutYear: int, CheckoutMonth: int, Checkouts: int, Title: chararray, isbn: chararray, Creator: chararray, Subjects: chararray, Publisher: chararray, PublicationYear: chararray);
c1 = FOREACH Checkout GENERATE CheckoutYear, CheckoutMonth, Title, Checkouts;
c2 = FOREACH c1 GENERATE REPLACE(Title,' ','') as Title, CheckoutYear, CheckoutMonth, Checkouts;
c3 = FOREACH c2 GENERATE REPLACE(Title, '([^a-zA-Z\\s]+)', '') as Title, CheckoutYear, CheckoutMonth, Checkouts;
c4 = FOREACH c3 GENERATE LOWER(Title) as Title, CheckoutYear, CheckoutMonth, Checkouts;

l = LOAD 's3://data/Normalized_Library_Collection_Inventory.csv' USING CSVLoader(',') as (Rank: int, Title: chararray, isbn: chararray);
l2 = FOREACH l GENERATE REPLACE(Title, '([^a-zA-Z\\s]+)', '') as Title, isbn;
l3 = FOREACH l2 GENERATE REPLACE(Title, ' ', '') as Title, isbn;
l4 = FOREACH l3 GENERATE LOWER(Title) as Title, isbn;
l5 = FOREACH l4 GENERATE REPLACE(isbn,'"','') as isbn, Title;

r = JOIN c4 BY (Title), l5 BY (Title);

Book = LOAD 's3://data/normalized_book32_listing copy.csv' USING CSVLoader(',') as (order: Int, isbn: chararray, Filename: chararray, ImageURL: chararray, Title: chararray, Author: chararray, CategoryID: chararray, Category: chararray);
b1 = FOREACH Book GENERATE REPLACE(isbn,'"','') as isbn, Category;

r2 = JOIN r BY (isbn), b1 BY (isbn);

Rain = LOAD 's3://data/rain_gauge_data_normalized_averaged.csv' USING PigStorage(',') as (year: int, month: int, AvgGauge: float);
f = JOIN r2 BY (CheckoutYear, CheckoutMonth), Rain BY (year, month);
f1 = DISTINCT f;
f2 = FILTER f1 BY (isbn != 'nan') AND (Title != '') AND (isbn != '');
f3 = GROUP f2 BY (Rain::year, Rain::month, r2::b1::Category, Rain::AvgGauge);
f4 = FOREACH f3 GENERATE FLATTEN(group) AS (year, month, cat, gauge), SUM(f.r2::r::c4::Checkouts) AS cnt;

order_by_data = ORDER f4 BY gauge DESC, cnt DESC;
STORE order_by_data INTO 's3://outputs' USING CSVExcelStorage();