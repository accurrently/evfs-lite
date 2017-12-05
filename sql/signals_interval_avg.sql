WITH sigs AS (
  SELECT
    AVG(CAST(Value AS Float64)) AS Value,
    CAST(UNIX_SECONDS(EventTime)/{interval} AS INT64) AS EventMinute,
    COUNT(*) AS SignalsNum
  FROM
    {table_name}
  WHERE
    VehicleID = "{vehicle_id}"
    AND {time_bracket}
  GROUP BY
    EventMinute
  ORDER BY
    EventMinute
  ASC
)
SELECT
  sigs.Value AS Value,
  TIMESTAMP_SECONDS(sigs.EventMinute*{interval}) AS EventTime,
  sigs.SignalsNum AS TotalSignals
FROM
  sigs
ORDER BY
  EventTime
ASC
