INSERT INTO NodePairs (
  SELECT *
  FROM (
         SELECT
           nodes.NodeId,
           directed.ToNodeId AS PairedNodeId
         FROM
           nodes, directed
         WHERE
           nodes.NodeId = directed.FromNodeId
         UNION
         SELECT
           nodes.NodeId,
           directed.FromNodeId AS PairedNodeId
         FROM
           nodes, directed
         WHERE
           nodes.NodeId = directed.ToNodeId
       ) AS i
);