REGISTER file:/usr/local/hadoop/hadoop-3.2.1/lib/pig/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

data = LOAD 'input/rain_gauge_data.csv' USING CSVExcelStorage();

-- Normalize the dates by extracting the year and month values into
-- their own columns, retaining the original date format as a join key
NormalizedData = FOREACH data GENERATE
  $0,
  GetYear(ToDate($0, 'MM/dd/yyyy')) AS Year,
  GetMonth(ToDate($0, 'MM/dd/yyyy')) AS Month;

-- Average the accumulation columns in each row to create a per-row average value
AveragedData = FOREACH data GENERATE $0, (
  (float)$1 +
  (float)$2 +
  (float)$3 +
  (float)$4 +
  (float)$5 +
  (float)$6 +
  (float)$7 +
  (float)$8 +
  (float)$9 +
  (float)$10 +
  (float)$11 +
  (float)$12 +
  (float)$13 +
  (float)$14 +
  (float)$15 +
  (float)$16 +
  (float)$17) / 17 AS RG_Global;

-- Join the normalized dates with the average values and generate a new relation
Merged1 = JOIN NormalizedData BY $0, AveragedData BY $0;
Result1 = FOREACH Merged1 GENERATE NormalizedData::Year, NormalizedData::Month, AveragedData::RG_Global;

-- Join the normalized dates with the original data set and generate a new relation
-- without the original data format columns
Merged2 = JOIN NormalizedData BY $0, data BY $0;
Result2 = FOREACH Merged2 GENERATE $1, $2, $4..;

STORE Result1 INTO 'results/averaged' USING CSVExcelStorage();
STORE Result2 INTO 'results/normalized' USING CSVExcelStorage();
