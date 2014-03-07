
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.FileSystem;
import java.util.HashMap;
import java.util.Map;



import java.sql.DriverManager;
import java.sql.SQLException;

import com.google.gson.Gson;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

import com.mysql.jdbc.ResultSet;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProviderNewImpl;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchRelationship;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


///
/// ***** SHOULD BE USED ONLY FOR INITIAL DB LOAD *****
///

public class Neo4jBatchInsertRelations 
{
	public static void main( String[] args )
	{
		final String DB_PATH="/Users/krishna/Developer/neo4j-enterprise-2.0.0-M06/data/graph.db";
		final String PROPERTY_FILE="/Users/krishna/Developer/workspace/Proj_Neo4j/neo4j-properties.txt";
		BatchInserter inserter=null;
		BatchInserterIndexProvider indexProvider=null;
		BatchInserterIndex relations=null;
		BatchInserterIndex pages = null;
		GraphDatabaseService graph=null;
		Connection connection = null;
		PreparedStatement pst = null;
		ResultSet rs = null;


		try {

			long start = System.currentTimeMillis();

			DefaultFileSystemAbstraction fileSystem=new DefaultFileSystemAbstraction();

			//Open Database Batch Insert
			InputStream property_file = fileSystem.openAsInputStream( new File( PROPERTY_FILE ) );
			Map<String, String> config = MapUtil.load( property_file );
			final Label pageLabel = DynamicLabel.label( "Page" );

			inserter = BatchInserters.inserter( DB_PATH,fileSystem,config);
			indexProvider =new LuceneBatchInserterIndexProvider( inserter );
			pages =indexProvider.nodeIndex( "Page", MapUtil.stringMap( "type", "exact" ) );
			relations = indexProvider.relationshipIndex( "Linked", MapUtil.stringMap( "type", "exact" ) );
			pages.setCacheCapacity( "id", 100000 );
			relations.setCacheCapacity( "key", 100000 );
			registerShutdownHook( inserter,indexProvider,pages, relations );

			Map<String, Object> fromPageParams = new HashMap<>();
			Map<String, Object> parameters = new HashMap<>();
			RelationshipType linkedto = DynamicRelationshipType.withName( "LINKEDTO" );

			//Get all "Default" type nodes
			final IndexHits<Long> fromPages=pages.get("ns", new String ("Default"));
			Iterable<Long> relationlist;

			//Open the mysql connection
			try {
				Class.forName("com.mysql.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				System.out.println("Where is your MySQL JDBC Driver?");
				e.printStackTrace();
				return;
			}

			try {
				connection = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/Wiki","root", "");
				pst = (PreparedStatement) connection.prepareStatement("SELECT * FROM pagelinks where pl_from= (?)");

			} catch (SQLException e) {
				System.out.println("Connection Failed! Check output console");
				e.printStackTrace();
				return;
			}
			if(connection!=null){
				int totalrelations=0;
				for(Long fromNode:fromPages){
					//Get the properties of "from node"
					fromPageParams=inserter.getNodeProperties(fromNode);

					//find all relations of "from node"
					relationlist=inserter.getRelationshipIds(fromNode);
					String pageid=fromPageParams.get(new String("id")).toString();
					String pagetitle=fromPageParams.get(new String("title")).toString();
					System.out.println("Node Id:"+fromNode.toString()+"\tFrom Page :"+ pageid + " - " + pagetitle );

					if(pageid!=null){
						pst = (PreparedStatement) connection.prepareStatement("SELECT * FROM pagelinks where pl_from= " + pageid +";");
						rs = (ResultSet) pst.executeQuery();

						while (rs.next()){
							//Get the node id for the page
							String toPageTitle=rs.getString("pl_title");
							//Create a relationship key as fromPageId+toPaegTitle
							String relKey=pageid + toPageTitle;

							IndexHits<Long> toPages=pages.get("title", toPageTitle);
							Long linktype=rs.getLong("pl_namespace");

							//Check if the relation already exists  --- This will allow us to run this code, after loading a new set of pages.
							IndexHits<Long> existingRelations=relations.get("key", new String(relKey));
							
							//Insert only of the relation does not exist.
							if(!existingRelations.iterator().hasNext()){
								for(Long topage:toPages){
									parameters.put( "type", linktype );
									parameters.put( "key", relKey );
									Long newRelation=inserter.createRelationship( fromNode, topage, linkedto, parameters);
									relations.add(newRelation,parameters);
									totalrelations++;
									System.out.println("\t\t ->"+topage.toString() +"\t"+toPageTitle);
								}
							}
							else
								System.out.println("Relationship between " + pagetitle +" and "+toPageTitle +" exists. ");
							//rs.close();

						}
					}
					if(totalrelations!=0 && totalrelations%10000==0){
						System.out.println("+++++++++++++++++ Flushing indexes..... +++++++++++++++++++");
						relations.flush();
						indexProvider.shutdown();
						indexProvider =new LuceneBatchInserterIndexProvider( inserter );
						relations = indexProvider.relationshipIndex( "Linked", MapUtil.stringMap( "type", "exact" ) );
						relations.setCacheCapacity( "key", 100000 );
						pages =indexProvider.nodeIndex( "Page", MapUtil.stringMap( "type", "exact" ) );
						pages.setCacheCapacity( "id", 100000 );
						System.out.println("+++++++++++++++++ Flushed Indexes+++++++++++++++++++");
						
					}
				}
				
				relations.flush();
			}
			try {
				if(connection!=null){

					connection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}


			long finish = System.currentTimeMillis();
			long totalTime = finish - start;
			System.out.println("TOTAL TIME " + totalTime);
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		finally{
			System.out.println("Shutting down ...");
			System.out.println("Flushing Relation Indexes.....");
			relations.flush();
			System.out.println("Shuttingdown indexer.....");
			indexProvider.shutdown();
			inserter.shutdown();
			System.out.println("Shutdown Complete....  .");
		}


	}

	private static void registerShutdownHook( final BatchInserter inserter, final BatchInserterIndexProvider indexProvider, final BatchInserterIndex pages, final BatchInserterIndex relations )
	{
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				try{
					pages.flush();
					relations.flush();
					indexProvider.shutdown();
					inserter.shutdown();
				}
				catch (Exception Ex){
					System.out.println ("Inserter seems to be shutdown");
				}
			}
		} );
	}
}