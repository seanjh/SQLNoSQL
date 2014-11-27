MATCH (startNode:Endpoint)-[:CONNECTED*0..9]-(connectedNode:Endpoint) WHERE startNode.Id = {value} RETURN startNode, COUNT(DISTINCT connectedNode)
