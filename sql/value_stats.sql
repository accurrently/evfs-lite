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
    VehicleID = "{vehicle_id}"
    AND {time_bracket}
)
LIMIT 1
