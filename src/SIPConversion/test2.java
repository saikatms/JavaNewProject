package SIPConversion;
import java.util.*;
public class test2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ArrayList<ArrayList<String>> arr = new ArrayList<ArrayList<String>>();
		
		int maxindex = 100;
		for (int i=0; i< maxindex; i++) 
			arr.add(new ArrayList<String>() {{ add(""); }});
		
		arr.get(15).add("Arunavo Banerjee");
		arr.get(15).add("Anupam Mondal");
		arr.get(15).add("Saikat Mondal");
		
		
		for (int i = 0 ; i< 100; i++)
			for(String item : arr.get(i))
				if(!item.isBlank())
					System.out.println(item);
		
		
	}

}
