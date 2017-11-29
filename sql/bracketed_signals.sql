SELECT
      Value,
      EventTime
  FROM
      {table_name}
  WHERE (
      (EventTime >= {begin_bracket})
      AND (EventTime <= {end_bracket})
      AND (VehicleID = "{vehicle_id}")
  )
  ORDER BY
      EventTime
  {order}
