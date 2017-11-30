SELECT
    MIN(FLOAT(Value)) AS Min,
    MAX(FLOAT(Value)) AS Max,
    Max - Min AS Delta,
    SUM(FLOAT(Value)) AS Sum,
    AVG(FLOAT(Value)) AS Avg,
    STDDEV_POP(FLOAT(Value)) AS PopStdDev,
    STDDEV_SAMP(FLOAT(Value)) AS SampStdDev
FROM
    {table_name}
WHERE (
    (EventTime >= {begin_bracket})
    AND (EventTime <= {end_bracket})
    AND (VehicleID = "{vehicle_id}")
)
LIMIT 1
