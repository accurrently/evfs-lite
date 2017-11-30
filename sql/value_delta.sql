SELECT
    MAX(FLOAT(Value)) - MIN(FLOAT(Value)) AS Delta,
FROM
    {table_name}
WHERE (
    (EventTime >= {begin_bracket})
    AND (EventTime <= {end_bracket})
    AND (VehicleID = "{vehicle_id}")
)
LIMIT 1
