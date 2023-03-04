-- ** Aggregate (COUNT, AVG, etc.) + Sliding time window **
-- Performs function on the aggregate rows over a 10 second sliding window for a specified column. 
--          .----------.   .----------.   .----------.              
--          |  SOURCE  |   |  INSERT  |   |  DESTIN. |              
-- Source-->|  STREAM  |-->| & SELECT |-->|  STREAM  |-->Destination
--          |          |   |  (PUMP)  |   |          |              
--          '----------'   '----------'   '----------'               
-- STREAM (in-application): a continuously updated entity that you can SELECT from and INSERT into like a TABLE
-- PUMP: an entity used to continuously 'SELECT ... FROM' a source STREAM, and INSERT SQL results into an output STREAM
-- Create output stream, which can be used to send to a destination
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" ("read_date" VARCHAR(16), "heating_and_cooling" DECIMAL);
-- Create a pump which continuously selects from a source stream (SOURCE_SQL_STREAM_001)
-- performs an aggregate count that is grouped by columns ticker over a 10-second sliding window
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
-- COUNT|AVG|MAX|MIN|SUM|STDDEV_POP|STDDEV_SAMP|VAR_POP|VAR_SAMP)
SELECT STREAM "read_date", AVG("submeter3_heatcool") OVER TEN_SECOND_SLIDING_WINDOW AS "heating_and_cooling"
FROM "SOURCE_SQL_STREAM_001"
-- Results partitioned by ticker_symbol and a 10-second sliding time window 
WINDOW TEN_SECOND_SLIDING_WINDOW AS (
  PARTITION BY "read_date"
  RANGE INTERVAL '10' SECOND PRECEDING);


