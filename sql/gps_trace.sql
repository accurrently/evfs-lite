WITH lat AS (
  SELECT
    AVG(CAST(Value AS Float64)) AS Value,
    CAST(UNIX_SECONDS(EventTime)/{interval} AS INT64) AS EventMinute,
    COUNT(*) AS SignalsNum
  FROM
    {lat_table}
  WHERE
    VehicleID = "{vehicle_id}"
    AND EventTime >= {begin_bracket}
    AND EventTime <= {end_bracket}
  GROUP BY
    EventMinute
  ORDER BY
    EventMinute
  ASC
),
lon AS (
  SELECT
    AVG(CAST(Value AS Float64)) AS Value,
    CAST(UNIX_SECONDS(EventTime)/{interval} AS INT64) AS EventMinute,
    COUNT(*) AS SignalsNum
  FROM
    {long_table}
  WHERE
  VehicleID = "{vehicle_id}"
  AND EventTime >= {begin_bracket}
  AND EventTime <= {end_bracket}
  GROUP BY
    EventMinute
  ORDER BY
    EventMinute
  ASC
)
SELECT
  lat.Value AS Latitude,
  lon.Value AS Longitude,
  TIMESTAMP_SECONDS(lat.EventMinute*{interval}) AS TraceTime,
  lat.SignalsNum + lon.SignalsNum AS TotalSignals
FROM
  lat, lon
WHERE
  lat.EventMinute = lon.EventMinute
ORDER BY
  lat.EventMinute
ASC
