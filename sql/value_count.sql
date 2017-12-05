SELECT
  COUNT(*)
FROM
  {table_name}
WHERE (
  VehicleID = "{vehicle_id}"
  AND {time_bracket}
  AND Value = {value}
)
LIMIT
