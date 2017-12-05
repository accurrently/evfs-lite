SELECT
    MAX(FLOAT(Value)) - MIN(FLOAT(Value)) AS Result,
FROM
    {table_name}
WHERE (
    VehicleID = "{vehicle_id}"
    AND {time_bracket}
)
LIMIT 1
