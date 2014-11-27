START (startNode )
MATCH startNode-[:CONNECTED*0..8]-endNode
RETURN startNode, COUNT(DISTINCT endNode)
