SELECT
    MIN(FLOAT(Value)) AS Min,
FROM
    {table_name}
WHERE (
    VehicleID = "{vehicle_id}"
    AND {time_bracket}
)
LIMIT 1
