import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.mysql.jdbc.Connection;



public class PageLinksUtil {
	private static Connection conn ;
	private static Map<String,Integer> map ;
	
	
	@SuppressWarnings("null")
	public static void main(String[] args) {
		PageLinks links = new PageLinks();
			links.initConnection();
			conn = links.getConnection();
			map = new HashMap<String,Integer>();
		//links.initConnection();
		ReadXMLUTF8FileSAX sx = new ReadXMLUTF8FileSAX();
		PageStructure struct = new  PageStructure(); 
		ResultSet set = sx.getPageIds();
		ArrayList<String> title = new ArrayList<String>();
		//Iterator<Row> itr = set.iterator();
		List<Row> itr = set.all();
		for (int i=10;i<itr.size();i++){
			Row row = itr.get(i);
			if(row!=null){
			String query ="Select * from Wiki.pagelinks where pl_from="+row.getInt("pageid")+";";
			try {
			com.mysql.jdbc.PreparedStatement pst   =   (com.mysql.jdbc.PreparedStatement) conn.prepareStatement(query);
				com.mysql.jdbc.ResultSet rs =(com.mysql.jdbc.ResultSet) pst.executeQuery();
				
				if(rs==null){
					continue;
				}
				while(rs.next()){
				//System.out.println("here");
				String str = rs.getString("pl_title");
				ResultSet rs1 = null;
				if (str!= null){
					str = str.replace("\'", "");
					
					rs1 = sx.getId(str);
				}
				if(rs1 == null)
					System.out.println("null returned");
				 List<Row> itr1 = rs1.all();
				 
				for(Row row1 : itr1) {
					//System.out.println("Row values  :  " + ii + " -- " + row1.getInt("pageid"));
					int pageid = row1.getInt("pageid");
					if(str != null)
					map.put(str,pageid);
				}
				}
				sx.insertTitle(i,map);
				map.clear();;
				
			} catch (SQLException e) {
				e.printStackTrace();
			}
			}
		}
		for(Row row : set){
			struct.setPageid(row.getInt("pageid"));
			System.out.println(row.getInt("pageid"));
		}
	
	}

}
