SELECT
    MAX(FLOAT(Value)) - MIN(FLOAT(Value)) AS Distance,
FROM
    {the_table}
WHERE (
    (EventTime >= {begin_bracket})
    AND (EventTime <= {end_bracket})
    AND (VehicleID = "{vehicle_id}")
    AND (Signal = "{odometer}")
)
LIMIT 1
