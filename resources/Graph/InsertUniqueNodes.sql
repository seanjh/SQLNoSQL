INSERT INTO nodes (
  SELECT NodeId
  FROM (
         SELECT DISTINCT FromNodeId AS NodeId
         FROM
           directed
         UNION
         SELECT DISTINCT ToNodeId AS NodeId
         FROM
           directed d
         WHERE NOT EXISTS (
             SELECT 1
             FROM directed d2
             WHERE d2.FromNodeId = d.ToNodeId
         )
       ) AS i
);