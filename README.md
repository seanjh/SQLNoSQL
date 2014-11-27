#Storing Trajectories and Graphs in NoSQL Database Management Systems (Assignment 4)
###Sean Herman (sjh293)

##Instructions
My project is divided into 2 main portions, each with their own respective executable.
###CreateDB
Execute the CreateDB.Main class file to create and populate all the prerequisite databases.

	java CreateDB.Main

A total of 5 databases are created by CreateDB: 2 MySQL (CA road graph and trajectories), and 1 for each of Neo4j (graph), MongoDB (trajectories), and Redis (trajectories). All but the Redis database can be created and populated with the full dataset within a few minutes.

This program requires that full, absolute path pointing to the 2 source data sets, be added to the graphSourceFileName and trajectorySourceDir static variables within in the Main class. The graph source should be a .txt file in the format of roadNet-CA.txt. The trajectories source should be the directory which contains all the numbered directories, which themselves include the /Trajectory directory and the numerous .plt files.

###HW4Queries
The queries must all be executed after the CreateDB.Main executable successfully completes (the Redis portion of CreateDB in particular takes a while). After the databases are created, the queries may all be executed using the following command.

	java HW4Queries.Main

##Storage Description
###Graphs
Nothing has changed dramatically in the implementation of my database, as compared to HW2. To optimize the MySQL queries, I do create a few additional helper tables beyond just the simple From/To node table.
The Neo4j database is simply represented by nodes, with a parameter containing the original/source node Id value. Connected nodes are associated with a relationship labelled CONNECTED.
###Trajectories
####MySQL
In MySQL, the trajectory data was spread across 3 tables: Users (i.e., the numbered directories), Sets (1 record per .plt file), and Measures (1 record per .plt file line).
####MongoDB
I had a lot of difficulty getting queries working with Mongo with a lot of document nesting, so I opted to exclude the user Id values from this database. I decided to forgo references entirely, and embed the measure objects within their respective trajectory set.
####Redis
Populating and designing the Redis database was more challenging than the rest, in my experience. I don't see any easy way to step cleanly through nested objects, so I opted for an approach that would never require more than a Key query.
All the GPS measures were stored together in one large hashmap. Each measure is identified by the string "measure:<num>", where *num* is a unique identifier for that measure. These measures were added to a list, beneath their respective trajectory set. The trajectory set key is simply the filename for the .plt file (though I suspect this could possibly cause conflicts between user directories). Finally, in order to optimize the "Number of measures on some day" query, measure keys are also added list under the date of the measure. The key for this date hashmap is simply "date:yyyyMMdd". Embedding these measure keys in a few different places is redundant, but it does make the Redis queries extremely performant.

##Query Performance Results
TBD... preliminary Trajectory results below.
MySQL Trajectory Measures on Date
	Max:28.978 seconds
	Min:17.406 seconds
	Mean:23.192 seconds
MongoDB Trajectory Measures on Date
	Max:173.704 seconds
	Min:147.662 seconds
	Mean:160.683 seconds
Redis Trajectory Measures on Date
	Max:0.032 seconds
	Min:0.017 seconds
	Mean:0.025 seconds

##Dependencies
Java 1.8 (language level 8.0)
* mongo-java-driver-2.12.4
* jedis-1.5.0
* mysql-connector-java-5.1.34
* neo4j-community-2.1.5
The exact versions used in this assignment may be downloaded at https://www.dropbox.com/sh/o3y67nwu9of4epc/AAA13VXzEF5JrkFCIgBO6q3qa?dl=0

Note: Alap Parikh <akp76@cornell.edu> lent me a hand with the Neo4j reachability logic. I had written a version in Cypher (included in /resources) that could not complete in a reasonable time.
