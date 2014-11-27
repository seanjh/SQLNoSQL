  SELECT 
    s.startTime, COUNT(m.measureId) AS measureCount 
  FROM  
    Sets s  
  INNER JOIN  
    Measures m ON m.setId = s.setId   
  WHERE   
    s.startTime = ? 
    AND s.userId = ?
  GROUP BY s.startTime;
