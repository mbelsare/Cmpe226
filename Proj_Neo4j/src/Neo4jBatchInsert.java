
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.FileSystem;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
 

///
/// ***** SHOULD BE USED ONLY FOR INITIAL DB LOAD *****
///

public class Neo4jBatchInsert 
{
    public static void main( String[] args )
    {
    	 final String DB_PATH="/Users/krishna/Developer/neo4j-enterprise-2.0.0-M06/data/graph.db";
		 final String PROPERTY_FILE="/Users/krishna/Developer/workspace/Proj_Neo4j/neo4j-properties.txt";
		 final String WIKIDUMP_FILE="/Volumes/GDrive_Mac/WikiDumps/enwiki-20131104-pages-meta-current24.xml";
		 //final String WIKIDUMP_FILE="/Users/krishna/Downloads/enwiki/enwiki-20131104-pages-meta-current24.xml";
    	try {
    		 
    		  long start = System.currentTimeMillis();
    	      SAXParserFactory factory = SAXParserFactory.newInstance();
    	      SAXParser saxParser = factory.newSAXParser();
    	      final StringBuilder textBuilder=new StringBuilder();
    	      final NamespaceTable table = new NamespaceTable();
    	      DefaultFileSystemAbstraction fileSystem=new DefaultFileSystemAbstraction();
    	      
    	      //Open Database Batch Insert
    	      InputStream property_file = fileSystem.openAsInputStream( new File( PROPERTY_FILE ) );
    	      Map<String, String> config = MapUtil.load( property_file );
    	      final BatchInserter inserter = BatchInserters.inserter( DB_PATH,fileSystem,config);
    	      final Label pageLabel = DynamicLabel.label( "Page" );
    	      //inserter.createDeferredSchemaIndex( pageLabel ).on( "id" ).create();
    	      
    	      final BatchInserterIndexProvider indexProvider =
    	              new LuceneBatchInserterIndexProvider( inserter );
    	      final BatchInserterIndex pages =
    	    	        indexProvider.nodeIndex( "Page", MapUtil.stringMap( "type", "exact" ) );
    	      pages.setCacheCapacity( "id", 100000 );

    	      registerShutdownHook( inserter,indexProvider,pages );
    	      
    	      DefaultHandler handler = new DefaultHandler() {
    	        boolean page = false;
    	        boolean title = false;
    	        boolean ns = false;
    	        boolean redirect = false;
    	        boolean revision = false;
    	        boolean parentid= false;
    	        boolean id = false;
    	        boolean contributor = false;
    	        boolean username = false;
    	        boolean comment = false;
    	        boolean text = false;
    	        boolean comments = false;
    	        
    	        Map<String, Object> parameters = new HashMap<>();
    	        
    	        String attributeValue =  null;
 
    	        public void startElement(String uri, String localName,
    	            String qName, Attributes attributes)
    	            throws SAXException {
 
    	          if (qName.equalsIgnoreCase("page")) {
    	        	  page = true;
    	          }
 
    	          else if (qName.equalsIgnoreCase("title")) {
    	        	  title = true;
    	          }
 
    	          else if (qName.equalsIgnoreCase("ns")) {
    	        	  ns = true;
    	          }
 
    	          else if (qName.equalsIgnoreCase("redirect")) {
    	        	 
    	        	  if(attributes.getValue("title")!= null){
    	        		  attributeValue = attributes.getValue("title").replace(" ", "_");
    	        		  
    	        	  }else{
    	        		  attributeValue = "";
    	        	  }
    	        	  redirect = true;
    	          }
    	         
    	          else if (qName.equalsIgnoreCase("revision")) {
    	        	  
    	        	  revision = true;
    	          }
    	          else if (qName.equalsIgnoreCase("id")) {
    	        	  
    	        	  id= true;
    	          }
    	          else if (qName.equalsIgnoreCase("contributor")) {
    	        	  contributor = true;
    	          }
    	          else if (qName.equalsIgnoreCase("username")) {
    	        	  username = true;
    	          }
    	          
    	          else  if (qName.equalsIgnoreCase("comment")) {
    	        	  comment = true;
    	          }
    	          else if (qName.equalsIgnoreCase("text")) {
    	        	  text = true;
    	          }
    	          
    	          else if (qName.equalsIgnoreCase("comments")) {
    	        	  comments = true;
    	        	  }
    	        }
 
    	        public void endElement(String uri, String localName,
    	                String qName)
    	                throws SAXException {
 
    	        	if (qName.equalsIgnoreCase("page")) {
    	        		 long nodeId=inserter.createNode(parameters, pageLabel);
    	        		 pages.add(nodeId, parameters);
    	        		 System.out.println( "Node Id :" +nodeId+"   ,Page Id:"+ parameters.get(new String("id"))+"  ,Page Inserted :" + parameters.get(new String("title")));
    	        		 
    	        		 if (nodeId%50000==0){
    	        			 System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    	        			 System.out.println("Flushing Pages.....");
    	        			 pages.flush();
    	        			 System.out.println("Pages Flushed.");
    	        		 }
    	        		 page=false;
    	        		 revision=false;
    	        		 contributor=false;
    	        	}
    	        	if (qName.equalsIgnoreCase("text")) {
    	        		
    	        		String finaltext = textBuilder.toString();
    	        		finaltext = finaltext.replaceAll("[\\r\\n]", "    ");
    	        		finaltext = finaltext.replace("\""," ");
    	        		finaltext = finaltext.replace("'"," ");
    	        		finaltext = finaltext.replace("\\"," ");
    	        		parameters.put( "text",finaltext.toString());
    	        		textBuilder.setLength(0);
    	        		 text=false;
    	        	
    	        	}	
    	        }
 
				public void characters(char ch[], int start, int length)
    	            throws SAXException {

    	          if (title) {
    	            title = false;
    	            parameters.put( "title", new String(ch,start,length) );
    	          }
 
    	          if (ns) {
    	              String nameSpaceValue = table.getNameSpaceMap().get(new String(ch, start, length));
    	              ns = false;
    	              parameters.put( "ns", nameSpaceValue.toString() );
    	          }
 
    	          if (redirect) {
    	              redirect = false;
    	              parameters.put( "redirect", attributeValue.toString());
    	              attributeValue="";
    	           }
    	          
    	          if(id && page){
    	        	  if(!revision){
    	        		  	//System.out.println("Current Id  : " + new String(ch,start,length));
    	        		  	if(StringUtils.isNumeric(new String(ch,start,length)))
    	        		  			parameters.put( "id",new String(ch,start,length));
    	        		  	else
    	        		  		parameters.put( "id",0);
    	    	              id= false;
    	        	  }
    	        	  
    	          }
    	          
    	           if(username){
	    	              username=false;
	    	              contributor=false;
	    	              parameters.put( "username", new String(ch,start,length));
	        	  }
    	          
    	           if(text){
  
 	    	             for(int i=start;i<start+length;i++){
 	    	            	 textBuilder.append(ch[i]);
 	    	             }
 	        	  }
    	           if(comment){
  	        		String commentTag = new String(ch, start, length);
  	        		int commentCount=0;
  	        		 if(commentTag != null){
  	        			commentCount = StringUtils.countMatches("[[User", commentTag);
  	        			 if(commentCount < 1){
  	        				 commentCount =1;
  	        			 }
  	        		 }
  	        		 else 
  	        			 commentTag="";
  	    	       comment=false;
  	    	       parameters.put( "comment", commentTag.toString() );
  	    	       parameters.put( "commentcount",commentCount);
  	    	      
  	        	  }
    	          
    	        }
 
    	      };
    	      File file = new File(WIKIDUMP_FILE);
    	      InputStream inputStream= new FileInputStream(file);
    	      Reader reader = new InputStreamReader(inputStream,"UTF-8");
 
    	      InputSource is = new InputSource(reader);
    	      is.setEncoding("UTF-8");
    	      saxParser.parse(is, handler);
    	      System.out.println("Shutting down ...");
    	      System.out.println("Flushing Pages.....");
    	      pages.flush();
    	      System.out.println("Shuttingdown indexer.....");
    	      indexProvider.shutdown();
    	      inserter.shutdown();
    	      System.out.println("Shutdown Complete....  .");
    	      
    	      long finish = System.currentTimeMillis();
    	      long totalTime = finish - start;
    	      System.out.println("TOTAL TIME " + totalTime);
    	      
    	    } catch (Exception e) {
    	      e.printStackTrace();
    	    }
 
    }
    
	private static void registerShutdownHook( final BatchInserter inserter, final BatchInserterIndexProvider indexProvider, final BatchInserterIndex pages )
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