WITH offsignals AS (
      SELECT
          VehicleID,
          EventTime,
          Value
      FROM
          {table_name}
      WHERE (
          (EventTime >= {prebracket_begin})
          AND (EventTime <= {prebracket_end})
      )
      ORDER BY EventTime ASC
  ),
  a AS (
      SELECT
          Value,
          EventTime,
          ROW_NUMBER() OVER(ORDER BY EventTime) AS RN
      FROM
          offsignals ),
  b AS (
      SELECT
      a1.VehicleID,
      a1.Value,
      a1.EventTime,
      ROW_NUMBER() OVER(ORDER BY a1.EventTime) AS RN
      FROM
          a a1
      LEFT OUTER JOIN
          a a2
      ON
          a2.RN = a1.RN - 1
      WHERE
          (a1.Value != a2.Value) OR (a2.RN IS NULL)
  )
  SELECT
      b1.VehicleID AS VehicleID,
      b1.Value AS Value,
      b1.EventTime AS StartTime,
      b2.EventTime AS EndTime,
      timestamp_diff(b2.EventTime,b1.EventTime, SECOND) AS Duration
  FROM
      b b1
  LEFT OUTER JOIN
      b b2
  ON
      b2.RN = b1.RN + 1
  WHERE
      b1.Value = {value}
      AND b1.VehicleID = "{vehicle_id}"
  ORDER BY
      b1.EventTime
