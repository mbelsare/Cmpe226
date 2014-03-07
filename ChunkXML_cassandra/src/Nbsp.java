
public class Nbsp {

	public static void main(String[] args) {
		
		String s  = "&nbsp; Rahul &abcd;";
		String str  = s.replace("&[a-zA-Z];", "");
		System.out.println(str);
	}

}
