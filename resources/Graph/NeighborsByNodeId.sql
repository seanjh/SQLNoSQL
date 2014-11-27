SELECT NodeId, COUNT(PairedNodeId) AS pairCount FROM NodePairs GROUP BY NodeId HAVING COUNT(PairedNodeId) > ?
