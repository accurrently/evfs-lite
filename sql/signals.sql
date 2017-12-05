SELECT
      Value,
      EventTime
  FROM
      {table_name}
  WHERE (
      VehicleID = "{vehicle_id}"
      AND {time_bracket}
  )
  ORDER BY
      EventTime
  {order}
