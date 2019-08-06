package SIPConversion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.opencsv.CSVReader;

public class SIP2CSVWithhandleId {
	public static int count;
	static int counter = 1;
	private static Map<String, Integer> nodeindexmap = new HashMap<String, Integer>();
	private static Set<String[]> rowset = new HashSet<String[]>();
	private static Set<String[]> subrowset = new HashSet<String[]>();
	private static List<String> headerlist = new ArrayList<String>();
	private static List<String> handleidlist = new ArrayList<String>();
	//	private static String handlePath = "/home/ndl/Desktop/data/WEL/config/handle_id.csv";

	private static String outputPath = "/home/ndl/Desktop/list1/test_csv/mycsv/";
	private static int thresold=10;
	private static int low=0;
	private static int high=thresold;

	public static void main(String[] args) throws Exception {
		System.out.println("Program started ...");
		long start = System.currentTimeMillis();
		String runType = args[0];
		String columnOrhandlePath=args[1];
		File source = new File(args[2]);
		if(!(runType.equalsIgnoreCase("-c") || runType.equalsIgnoreCase("-l")))
			throw new Exception("Wrong Runtype. Runtype in -l[handle list]/-c[column]].");

		if(args.length == 3) {
			if(runType.equalsIgnoreCase("-l")) {
				CSVReader handleCsv = new CSVReader(new FileReader(columnOrhandlePath));
				for (String[] row : handleCsv.readAll()) {
					handleidlist.add(row[0]);
				}
				handleCsv.close();
			}
			if(runType.equalsIgnoreCase("-c")) {
				CSVReader headerCsv = new CSVReader(new FileReader(columnOrhandlePath));
				for (String[] row : headerCsv.readAll()) {
					headerlist.add(row[0]);
				}
				headerlist.add(0, "WEL_V2/ID");
				headerCsv.close();
			}
		}
		if(args.length==5) {

			String runType2=args[3];
			String columnPath=args[4];
			System.out.println(runType2);
			System.out.println(columnPath);
			if(runType.equalsIgnoreCase("-l")) {
				CSVReader handleCsv = new CSVReader(new FileReader(columnOrhandlePath));
				for (String[] row : handleCsv.readAll()) {
					handleidlist.add(row[0]);
				}
				handleCsv.close();
			}
			if(runType2.equalsIgnoreCase("-c")) {
				CSVReader headerCsv = new CSVReader(new FileReader(columnPath));
				for (String[] row : headerCsv.readAll()) {
					headerlist.add(row[0]);
				}
				headerlist.add(0, "WEL_V2/ID");
				headerCsv.close();

			}

		}

		System.out.println(handleidlist);
		System.out.println(headerlist);

		traverse(source);
		multiplexmltocsv(outputPath,count);

		long end = System.currentTimeMillis();
		System.out.println("Finished in " + (end - start) / (1000 * 60) + "m");
	}
	public static void multiplexmltocsv2(String path,int count) {
		String pathName=path;
		String csvName = path+count+".csv";
		System.out.println("path name"+csvName);
		File csvFile = new File(csvName);
		if (!csvFile.getParentFile().exists())
			csvFile.getParentFile().mkdirs();
		String[] header; 

		header = new String[nodeindexmap.size()]; 		
		for (Map.Entry<String, Integer> entry : nodeindexmap.entrySet()) {
			//		    System.out.println(entry.getKey()+" : "+entry.getValue());
			header[entry.getValue()]=entry.getKey();

		}
		try {
			
			FileWriter fW = new FileWriter(csvFile);
			CSVPrinter csvPrinter = new CSVPrinter(fW, CSVFormat.DEFAULT.withHeader(header));
			for (String[] onerow : rowset) {
				csvPrinter.printRecord(Arrays.asList(onerow));
			}
			
			csvPrinter.close();
			rowset.clear();
		}
		catch(Exception e){
			e.printStackTrace();
		}    	
	}

	public static void multiplexmltocsv(String path,int count) {
		String pathName=path;
		String csvName = path+count+".csv";
		System.out.println("path name"+csvName);
		File csvFile = new File(csvName);
		if (!csvFile.getParentFile().exists())
			csvFile.getParentFile().mkdirs();

		String[] header = new String[nodeindexmap.size()];
		Integer[] subheader = new Integer[headerlist.size()];
		for (Map.Entry<String, Integer> entry : nodeindexmap.entrySet()) {
			header[entry.getValue()] = entry.getKey();
		}
		//Initialze subheader value
		for (int i = 0; i < headerlist.size(); i++) {
			int columnvalue = 0;
			for (Map.Entry<String, Integer> entry : nodeindexmap.entrySet()) {
				if (headerlist.get(i).toString().equals(entry.getKey().toString())) {
					columnvalue = entry.getValue();
				}
			}
			subheader[i] = columnvalue;
			//			System.out.println(i + "==>" + subheader[i]);
		}
		//copy value from original csv to subset of csv
		for (String[] onerow : rowset) {
			try {
				String[] newonerow = new String[headerlist.size()];

				for (int k = 0; k < headerlist.size(); k++) {
					newonerow[k] = onerow[subheader[k]];
				}
				subrowset.add(newonerow);

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
		}

		try {
			FileWriter fW = new FileWriter(csvFile);
			if (headerlist.size() > 0) {
				CSVPrinter csvPrinter = new CSVPrinter(fW,
						CSVFormat.DEFAULT.withHeader(headerlist.toArray(new String[0])));
				for (String[] myonerow : subrowset) {
					csvPrinter.printRecord(Arrays.asList(myonerow));
				}
				csvPrinter.close();
				subrowset.clear();
			} else {
				CSVPrinter csvPrinter = new CSVPrinter(fW, CSVFormat.DEFAULT.withHeader(header));
				for (String[] onerow : rowset) {
					csvPrinter.printRecord(Arrays.asList(onerow));
				}
				csvPrinter.close();
				rowset.clear();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void traverse(File parent) throws IOException {
		for (File input : parent.listFiles())
			if (input.isDirectory())
				traverse(input);
			else {
				String parent_name = parent.getAbsolutePath().replace("/home/ndl/Desktop/test_sip_data/", "");
				//				if(count==300) {
				//					break;
				//				}

				BufferedReader brTest = new BufferedReader(new FileReader(new File(parent + "/handle")));
				String handle = brTest.readLine();
				if(!headerlist.isEmpty() && handleidlist.isEmpty()) {
					System.out.println("Accesing File : " + count++ + " : " + parent_name);
					System.out.println("Hi alll header list");

					parseDublin(new File(parent + "/dublin_core.xml"));
					setvalueMap(new File(parent + "/dublin_core.xml"), handle);	
					if(count==high) {					
						multiplexmltocsv(outputPath,count);
						low=high;
						high=low+thresold;
					}
					break;

				}
				if(headerlist.isEmpty() && !handleidlist.isEmpty() && handleidlist.contains(handle)) {
					System.out.println("Accesing File : " + count++ + " : " + parent_name);
					parseDublin(new File(parent + "/dublin_core.xml"));
					setvalueMap(new File(parent + "/dublin_core.xml"), handle);						

					if(count==high) {					
						multiplexmltocsv2(outputPath,count);
						low=high;
						high=low+thresold;

					}
					System.out.println("count"+count);
					break;
				}
//				if(!headerlist.isEmpty() && !handleidlist.isEmpty() && handleidlist.contains(handle)) {
//					System.out.println("Accesing File : " + count++ + " : " + parent_name);
//					parseDublin(new File(parent + "/dublin_core.xml"));
//					setvalueMap(new File(parent + "/dublin_core.xml"), handle);	
//					if(count==high) {					
//						multiplexmltocsv(outputPath,count);
//						low=high;
//						high=low+thresold;
//
//					}
//					break;
//				}



			}
	}

	public static void setvalueMap(File xmlFile, String handle) {
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder;
			dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(xmlFile);
			doc.getDocumentElement().normalize();

			Element rootElement = doc.getDocumentElement();
			ArrayList rootElementlist = listAllAttributes(rootElement);
			String attrVal = rootElementlist.get(0).toString();

			String[] row = new String[nodeindexmap.size()+1];

			NodeList nList = doc.getDocumentElement().getChildNodes();
			for (int j = 0; j < nList.getLength(); j++) {
				if (nList.item(j).getNodeType() != Node.ELEMENT_NODE)
					continue;
				Element thisNode = (Element) nList.item(j);

				ArrayList attrlist = listAllAttributes(thisNode);
				int listSize = attrlist.size();

				String columnname = "";
				for (int k = 0; k < listSize; k++) {
					if (k == (listSize - 1)) {
						columnname = columnname + attrlist.get(k);
					} else {
						columnname = columnname + attrlist.get(k) + ".";
					}
				}
				columnname = attrVal + "." + columnname;

				String data = thisNode.getTextContent().trim();

				int columnindex = nodeindexmap.get(columnname);
				row[0] = handle;
				row[columnindex] = data;
				rowset.add(row);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void parseDublin(File dublin) {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);
			DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
			Document dcf = documentBuilder.parse(dublin);
			dcf.getDocumentElement().normalize();

			// Parse attributes of rootnode
			Element rootElement = dcf.getDocumentElement();
			ArrayList rootElementlist = listAllAttributes(rootElement);
			String attrVal = rootElementlist.get(0).toString();

			NodeList nList = dcf.getDocumentElement().getChildNodes();
			nodeindexmap.put("WEL_V2/ID", 0);
			for (int i = 0; i < nList.getLength(); i++) {
				Node nNode = nList.item(i);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					ArrayList attrlist = listAllAttributes(eElement);
					int listSize = attrlist.size();

					String columnname = "";
					for (int k = 0; k < listSize; k++) {
						if (k == (listSize - 1)) {
							columnname = columnname + attrlist.get(k);
						} else {
							columnname = columnname + attrlist.get(k) + ".";
						}
					}
					columnname = attrVal + "." + columnname;
					if (!nodeindexmap.containsKey(columnname)) {
						nodeindexmap.put(columnname, counter++);
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static ArrayList listAllAttributes(Element element) {

		// get a map containing the attributes of this node
		NamedNodeMap attributes = element.getAttributes();

		// get the number of nodes in this map

		int numAttrs = attributes.getLength();
		ArrayList<String> attributeslist = new ArrayList<String>();
		for (int i = 0; i < numAttrs; i++) {

			Attr attr = (Attr) attributes.item(i);
			String attrName = attr.getNodeName();
			String attrValue = attr.getNodeValue();
			attributeslist.add(attrValue);

		}
		return attributeslist;
	}
}
