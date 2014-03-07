import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


public class FileChunk {

	public static void main(String[] args) {
		
		
		try {
			
			long start = System.currentTimeMillis();
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
//			final StringBuilder builder = new StringBuilder().append("<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.8/\" "
//					+ "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
//					+ "xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.8/ "
//					+ "http://www.mediawiki.org/xml/export-0.8.xsd\" version=\"0.8\" xml:lang=\"en\">");
			final StringBuilder builder = new StringBuilder().append("<a>");
			final StringBuilder textBuilder = new StringBuilder();
			final NamespaceTable table = new NamespaceTable();
			DefaultHandler handler = new DefaultHandler(){
				int count =0;
				boolean mediawiki = false;
				boolean siteinfo = false;
				boolean base = false;
				boolean generator = false;
				boolean caseTag = false;
				boolean namespaces = false;
				boolean namespace = false;
				boolean page = false;
				boolean title = false;
				boolean ns = false;
				boolean redirect = false;
				boolean revision = false;
				boolean id = false;
				boolean parentid = false;
				boolean timestamp =false;
				boolean contributor = false;
				boolean username = false;
				boolean comments = false;
				boolean text = false;
				boolean a = false; 
				String attributeValue = null;
				int filecount =0;
				int savefilecount=0;
				
				
				 public void startElement(String uri, String localName,
		    	            String qName, Attributes attributes)
		    	            throws SAXException {
		 
					 
		    	          if (qName.equalsIgnoreCase("page")) {
		    	        	  builder.append("<page>");
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
		    	        		  attributeValue = attributeValue.replace("&", "");
		    	        		  
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
		    	        	  comments = true;
		    	          }
//		    	          else if (qName.equalsIgnoreCase("text")) {
//		    	        	  
//		    	        	  text = true;
//	    	          }
		    	          
		    	          
		    	        }
		 
		    	        public void endElement(String uri, String localName,
		    	                String qName)
		    	                throws SAXException {
		 
		    	        	if (qName.equalsIgnoreCase("page")) {
		    	        		 
		    	        		builder.append("</page>");
								count ++;
								System.out.println("Insde page " + count);
							
								//System.out.println(builder.toString());
								if(count %10000 == 0){
									
									
									if(filecount%20==0){
										boolean b = new File ("/Users/raul/Desktop/split/"+filecount).mkdir();
										savefilecount = filecount;
										
									}
									File f = new File ("/Users/raul/Desktop/split/"+savefilecount+"/"+filecount+".txt");
									filecount++;
									
									try {
										//BufferedWriter writer = new BufferedWriter(new FileWriter(f));
										//writer.write("rahul mehta");
										builder.append("</a>");
										FileUtils.writeStringToFile(f,builder.toString(),"UTF-8");
										builder.setLength(0);
//										builder.append("<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.8/\" "
//												+ "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
//												+ "xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.8/ "
//												+ "http://www.mediawiki.org/xml/export-0.8.xsd\" version=\"0.8\" xml:lang=\"en\">");
										builder.append("<a>");
									} catch (IOException e) {
										
										e.printStackTrace();
									}
								}
		    	        		 
		    	        		 page=false;
		    	        		 revision=false;
		    	        		 contributor=false;
		    	        	}
//		    	        	if (qName.equalsIgnoreCase("text")) {
//		    	        		
//		    	        		String finaltext = textBuilder.toString();
//		    	        		finaltext = finaltext.replaceAll("[\\r\\n]", "    ");
//		    	        		finaltext = finaltext.replace("\""," ");
//		    	        		finaltext = finaltext.replace("'"," ");
//		    	        		finaltext = finaltext.replace("\\"," ");
//		    	        		finaltext = finaltext.replace("&", "");
//		    	        		finaltext = finaltext.replace(";", "");
//		    	        		builder.append( "<text>"+finaltext.toString()+"</text>");
//		    	        		textBuilder.setLength(0);
//		    	        		 text=false;
//		    	        	
//		    	        	}	
		    	        }
		 
						public void characters(char ch[], int start, int length)
		    	            throws SAXException {

		    	          if (title) {
		    	            title = false;
		    	            builder.append( "<title>"+new String(ch,start,length)+"</title>");
		    	          }
		 
		    	          if (ns) {
		    	              String nameSpaceValue = table.getNameSpaceMap().get(new String(ch, start, length));
		    	              ns = false;
		    	              builder.append("<ns>"+nameSpaceValue.toString()+"</ns>");
		    	          }
		 
		    	          if (redirect) {
		    	              redirect = false;
		    	              builder.append( "<redirect>"+attributeValue.toString()+"</redirect>");
		    	              attributeValue="";
		    	           }
		    	          
		    	          if(id && page){
		    	        	  if(!revision){
		    	        		  	//System.out.println("Current Id  : " + new String(ch,start,length));
		    	        		  	if(StringUtils.isNumeric(new String(ch,start,length)))
		    	        		  			builder.append("<id>"+new String(ch,start,length)+"</id>");
		    	    	             
		    	        		  	id= false;
		    	        	  }
		    	        	  
		    	          }
		    	          
		    	           if(username){
			    	              username=false;
			    	              contributor=false;
			    	              builder.append( "<username>"+new String(ch,start,length)+"</username>");
			        	  }
		    	          
		    	           if(text){
		  
		 	    	             for(int i=start;i<start+length;i++){
		 	    	            	 textBuilder.append(ch[i]);
		 	    	             }
		 	        	  }
		    	           if(comments){
		  	        		String commentTag = new String(ch, start, length);
		  	        		commentTag = commentTag.replace("&", "_");
		  	        		commentTag = commentTag.replace("<", "");
		  	        		commentTag = commentTag.replace(">", "");
		  	        		int commentCount=0;
		  	        		 if(commentTag != null){
		  	        			commentCount = StringUtils.countMatches("[[User", commentTag);
		  	        			 if(commentCount < 1){
		  	        				 commentCount =1;
		  	        			 }
		  	        		 }
		  	        		 else {
		  	        			 commentTag="";
		  	        		 }
		  	        		 comments=false;
		  	    	       builder.append("<comment>"+commentTag.toString()+"</comment>" );
		  	    	       builder.append("<commentcount>"+commentCount+"</commentcount>" );
		  	    	       		  	    	      
		  	        	  }
		    	          
		    	        }
			
			
			};
			File file = new File("/Users/raul/Desktop/enwiki-20131104-pages-meta-current24.xml");
			InputStream inputStream = new FileInputStream(file);
			Reader reader = new InputStreamReader(inputStream, "UTF-8");
			InputSource is = new InputSource(reader);
			is.setEncoding("UTF-8");
			saxParser.parse(is, handler);

			long finish = System.currentTimeMillis();
			long totalTime = finish - start;
			
			System.out.println(totalTime);
//			System.out.println(textBuilder.toString());
		
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			}

}
