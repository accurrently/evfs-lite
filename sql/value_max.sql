SELECT
    MAX(FLOAT(Value)) AS Max,
FROM
    {table_name}
WHERE (
    (EventTime >= {begin_bracket})
    AND (EventTime <= {end_bracket})
    AND (VehicleID = "{vehicle_id}")
)
LIMIT 1
