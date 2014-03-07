import java.util.HashMap;
import java.util.Map;


public class NamespaceTable {
	private Map<String,String> nameSpaceMap;
	
	public NamespaceTable(){
		nameSpaceMap = new HashMap<String, String>();
		putNamespace();
	
	}

	private void putNamespace() {
		nameSpaceMap.put("-2","Media");
		nameSpaceMap.put("-1","Special");
		nameSpaceMap.put("0","Default");
		nameSpaceMap.put("1","Talk");
		nameSpaceMap.put("2","User");
		nameSpaceMap.put("3","User Talk");
		nameSpaceMap.put("4","Wikipedia");
		nameSpaceMap.put("5","Wikipedia Talk");
		nameSpaceMap.put("6","File");
		nameSpaceMap.put("7","File Talk");
		nameSpaceMap.put("8","MediaWiki");
		nameSpaceMap.put("9","MediaWiki Talk");
		nameSpaceMap.put("10","Template");
		nameSpaceMap.put("11","Template Talk");
		nameSpaceMap.put("12","Help");
		nameSpaceMap.put("13","Help Talk");
		nameSpaceMap.put("14","Category");
		nameSpaceMap.put("15","Category Talk");
		nameSpaceMap.put("100","Portal");
		nameSpaceMap.put("101","Portal Talk");
		nameSpaceMap.put("108","Book");
		nameSpaceMap.put("109","Book Talk");
		nameSpaceMap.put("446","Education Program");
		nameSpaceMap.put("447","Education Program Talk");
		nameSpaceMap.put("710","TimedText");
		nameSpaceMap.put("711","TimedText Talk");
		
	}

	public Map<String, String> getNameSpaceMap() {
		return nameSpaceMap;
	}

	public void setNameSpaceMap(Map<String, String> nameSpaceMap) {
		this.nameSpaceMap = nameSpaceMap;
	}
	
	
	
	
	
}
