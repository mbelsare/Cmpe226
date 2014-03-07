import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang3.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class ReadXMLUTF8FileSAX {
	private static Session session;
	private static Cluster cluster;
	static String cql = " BEGIN BATCH ";
	static int rowCount = 0;
	static BatchStatement batch = new BatchStatement();

	public static void main(String[] args) {
		try {
			createConnection();
			createSchema();
			createCoulmnFamily();
			// loadData();
			long start = System.currentTimeMillis();
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			final StringBuilder builder = new StringBuilder();
			final StringBuilder textBuilder = new StringBuilder();
			final NamespaceTable table = new NamespaceTable();
			DefaultHandler handler = new DefaultHandler() {
				
				boolean a = false;
				boolean page = false;
				boolean title = false;
				boolean ns = false;
				boolean redirect = false;
				boolean id = false;
				boolean username = false;
				boolean comment = false;
				boolean text = false;
				boolean commentCount = false;
				String attributeValue = null;
				String title_cql = null;
				String ns_cql = null;
				int count = 0;
				int pageid = 0;
				String redirect_cql = null;
				String username_cql = null;
				String comment_cql = null;
				int commentCount_cql;
				String text_cql = null;

				public void startElement(String uri, String localName,
						String qName, Attributes attributes)
						throws SAXException {
					
					if(qName.equalsIgnoreCase("a")){
						a = true;
					}

					if (qName.equalsIgnoreCase("page")) {
						System.out.println(count++);
						page = true;
					}

					if (qName.equalsIgnoreCase("title")) {
						title = true;
					}

					if (qName.equalsIgnoreCase("ns")) {
						ns = true;
					}

					if (qName.equalsIgnoreCase("redirect")) {
						redirect = true;
					}
					if (qName.equalsIgnoreCase("id")) {
						id = true;
					}
					
					if (qName.equalsIgnoreCase("username")) {
						username = true;
					}

					if (qName.equalsIgnoreCase("comment")) {
						comment = true;
					}
					if (qName.equalsIgnoreCase("commentCount")) {
						commentCount = true;
					}
					if (qName.equalsIgnoreCase("text")) {
						text = true;
					}

				}

				public void endElement(String uri, String localName,
						String qName) throws SAXException {

					if (qName.equalsIgnoreCase("page")) {
//
						// builder.append("}");
						// cql+=
						// " INSERT INTO wiki.page (title, ns, pageid, redirect,username,comment,"
						// +
						// "commentCount) VALUES ('"+title_cql+"','"+ns_cql+"',"+pageid+",'"+redirect_cql+"','"
						// +username_cql+"','"+comment_cql+"',"+commentCount_cql+"'); "
						// ;
						PreparedStatement prep = session
								.prepare(" INSERT INTO wiki.page (title, ns, pageid,redirect,username,comment,commentCount) "
										+ "VALUES (?,?,?,?,?,?,?) ");
						batch.add(prep.bind(title_cql, ns_cql, pageid,
								redirect_cql, username_cql, comment_cql,
								commentCount_cql));
						rowCount++;

						if (rowCount % 300 == 0) {
							// cql+=" APPLY BATCH ;";
							// System.out.println(cql+"\n");
							session.execute(batch);
//							try {
//								Thread.sleep(100);
//							} catch (InterruptedException e) {
//								// TODO Auto-generated catch block
//								e.printStackTrace();
//							}
							cql = "" + " BEGIN BATCH ";
						}
						page = false;
					}
					if (qName.equalsIgnoreCase("text")) {
						String finaltext = textBuilder.toString();// .replace("{{","##");
						// finaltext = finaltext.replace("}}", "#");
						finaltext = finaltext.replaceAll("[\\r\\n]", "    ");
						finaltext = finaltext.replace("\"", " ");
						finaltext = finaltext.replace("'", " ");
						finaltext = finaltext.replace("\\", " ");
						text_cql = finaltext;
						textBuilder.setLength(0);
						text = false;

					}
					

				}

				public void characters(char ch[], int start, int length)
						throws SAXException {

					if (title) {
						title = false;
						title_cql = new String(ch, start, length);
						
					}

					if (ns) {
						String nameSpaceValue = new String(ch, start, length);
						ns = false;
						ns_cql = nameSpaceValue;
					}

					if (redirect) {
						redirect = false;
						redirect_cql = new String(ch, start, length);
						
					}

					if (id) {
						pageid  = Integer.parseInt(new String(ch, start, length));
						id = false;
					}

					if (username) {
						username = false;
						username_cql = new String(ch, start, length);
					}

					if (text) {

						for (int i = start; i < start + length; i++) {
							textBuilder.append(ch[i]);
						}
					}
					if (comment) {
						comment_cql = new String(ch, start, length);
						comment = false;
					}
					if (commentCount) {
						commentCount_cql  = Integer.parseInt(new String(ch, start, length));
						commentCount = false;
					}

				}

			};

//			File file = new File(
//					"/Users/raul/Desktop/enwiki-20131104-pages-meta-current4.xml");
			// File file = new
			// File("/Users/raul/Downloads/Proj_Neo4j-1/enwiki-20131104-pages-meta-current1.xml");
//			File file = new File("/Users/raul/Desktop/2.txt");
			File file = new File(args[0]);
			System.out.println(args[0]);
			
			InputStream inputStream = new FileInputStream(file);
			Reader reader = new InputStreamReader(inputStream, "UTF-8");
			InputSource is = new InputSource(reader);
			is.setEncoding("UTF-8");

			saxParser.parse(is, handler);

			long finish = System.currentTimeMillis();
			long totalTime = finish - start;

			// System.out.println(builder.toString());
			// JSONObject jsonObj = XML.toJSONObject(builder.toString());
			// System.out.println(jsonObj);
			System.out.println("TOTAL TIME " + totalTime);
			System.exit(0);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void createConnection() {
		cluster = new Cluster.Builder().addContactPoint("localhost")
				.withPort(9042).build();
		session = cluster.connect();
	}

	public void insertTitle(int i ,Map <String,Integer> map)
	{
		//createConnection();
		String query = "create TABLE IF NOT EXISTS wiki.links(pageid int ,cols varchar,linkid int,PRIMARY KEY(pageid,cols,linkid));";
		
		session.execute(query);
		String insertQuery = "BEGIN BATCH ";
		for(String str : map.keySet()){
			insertQuery+=" Insert into wiki.links (pageid,cols,linkid) VALUES (" + i +",'" + str + "'," + map.get(str)+"); ";
			
		}
		insertQuery+=" APPLY BATCH ;";
		
		session.executeAsync(insertQuery);
	}
	
	private static void createCoulmnFamily() {

		String cqlStatement = "Create Table IF NOT exists wiki.page (title varchar , ns varchar , "
				+ "pageid int, redirect varchar , username varchar , comment varchar , "
				+ "commentCount int, PRIMARY KEY (pageid));";

		String indexstmnt = "create index if not exists wiki_page_title on wiki.page(title);";
		session.execute(cqlStatement);
		session.execute(indexstmnt);

	}

	private static void createSchema() {

		session.execute("CREATE KEYSPACE IF NOT exists wiki WITH replication ="
				+ "{'class':'SimpleStrategy', 'replication_factor':1}");
	}

	public ResultSet getPageIds() {
		createConnection();
		ResultSet rs = session.execute(" Select pageid from wiki.page ;");
		if (rs != null) {
			System.out.println("rs is not ull");
		}
		return rs;
	}
	
	public ResultSet getId(String str){
		//System.out.println(" +++ " + str + " ;;;");
		ResultSet rs = session.execute(" Select pageid from wiki.page where title='"+str+"' ; ");
		return rs ;
	}

}