WITH run_indicators AS
(
  SELECT
    UNIX_SECONDS(EventTime) AS ETime,
    Value,
    ROW_NUMBER() OVER (ORDER BY ETime) AS RowNum
  FROM
    {table_name}
  WHERE
    VehicleID = "{vehicle_id}"
  ORDER BY
    ETime
  ASC
),
changes AS (
  SELECT
    r1.*
  FROM
    run_indicators r1
  LEFT OUTER JOIN
    run_indicators r2 ON r1.RowNum = r2.RowNum + 1
  WHERE
    r2.RowNum IS NULL
    OR r2.Value <> r1.Value
)
