SELECT
    MAX(FLOAT(Value)) AS Max,
FROM
    {table_name}
WHERE (
    VehicleID = "{vehicle_id}"
    AND {time_bracket}
)
LIMIT 1
