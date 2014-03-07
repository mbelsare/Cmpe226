How to load and execute the demo

All the required JAR files are here : https://www.dropbox.com/s/krgnietb1x7drrn/lib.zip

Data Sets
1. The Wikipedia page information in the XML file.
2. The MySQL dump has to be loaded onto the local MySQL instance.

Neo4j
1. Add all the jars in the Neo4j/lib folder into the project.
2. Update the XML file location in the Neo4jBatchInserter.java file.
3. Run the batch inserter.
4. Update MySQL connection information in the Neo4jRelationBatchInsert.java
5. Run the Relation batch inserter

Cassandra
1. Start the Cassandra database as a root user -- ./bin/Cassandra -f
2. Add all the jars into your build–path which is present inside the lib folder of the
project.
3. Pass the file name as a command line argument into the ReadXMLUTF8FileSAX.java.
4. Run the program to insert the data.
5. Run the PageLinksUtil.java to insert the data into the wiki.links column family which
would create the links for different pages in wikipedia

Running the Website
1. Copy the Wikigraph folder to your local machine 
$>cd Wikigraph
$>bundle
$>rackup
The website will be available at http://localhost:9292