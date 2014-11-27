  SELECT
    m.measureDateGMT AS measureDate,
    COUNT(m.measureId) AS measureCount
  FROM
    Measures m
  WHERE
    m.measureDateGMT = ?
  GROUP BY m.measureDateGMT;
