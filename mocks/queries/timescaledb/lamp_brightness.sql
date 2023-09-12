CREATE TABLE l1 (
    ts TIMESTAMP PRIMARY KEY,
    brightness FLOAT
);

-- SELECT brightness, time_bucket(INTERVAL '1 second', ts) AS bucket FROM l1;