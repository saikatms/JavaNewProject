package ndl.SIPToCSVFromTarGz;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.iitkgp.ndl.data.compress.CompressedDataItem;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileReader;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.opencsv.CSVReader;

public class SIP2CSVWithhandleID_v1 {
	public static int count;
	static int counter = 1;
	private static Map<String, Integer> nodeindexMap = new HashMap<String, Integer>();
	private static HashMap<String, ArrayList<String>> dataMap = new HashMap<String, ArrayList<String>>();
	private static Set<String[]> rowSet = new HashSet<String[]>();
	private static Set<String[]> subrowSet = new HashSet<String[]>();
	private static List<String> headerList = new ArrayList<String>();
	private static List<String> handleIDList = new ArrayList<String>();

	private static String outputpath = "/home/ndl/Desktop/Test/";
	private static int thresold = 100;
	private static int low = 0;
	private static int high = thresold;
	public static String p = "";

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Program Started Sir...");
		long start = System.currentTimeMillis();
		String runType = args[0];
		String columnOrHandlePath = args[1];
		File sourceFile = new File(args[2]);

		if (!(runType.equalsIgnoreCase("-c") || runType.equalsIgnoreCase("-l"))) {
			throw new Exception("Wrong Runtype. Runtype in -l[handle list]/-c[column].");
		}

		if (args.length == 3) {
			if (runType.equalsIgnoreCase("-l")) {
				CSVReader handleCsvReader = new CSVReader(new FileReader(columnOrHandlePath));
				for (String[] row : handleCsvReader.readAll()) {
					handleIDList.add(row[0]);
				}
				handleCsvReader.close();
			}

			if (runType.equalsIgnoreCase("-c")) {
				CSVReader headerCsvReader = new CSVReader(new FileReader(columnOrHandlePath));
				for (String[] row : headerCsvReader.readAll()) {
					headerList.add(row[0]);
				}
				headerList.add(0, "WEL_V2/ID");
				headerCsvReader.close();
			}
		}

		if (args.length == 5) {
			String runType2 = args[3];
			String columnPath = args[4];
//			System.out.println(runType2);
//			System.out.println(columnPath);

			if (runType2.equalsIgnoreCase("-l")) {
				CSVReader handleCsvReader = new CSVReader(new FileReader(columnPath));
				for (String[] row : handleCsvReader.readAll()) {
					handleIDList.add(row[0]);
				}
				handleCsvReader.close();
			}
			if (runType2.equalsIgnoreCase("-c")) {
				CSVReader headerCsvReader = new CSVReader(new FileReader(columnPath));
				for (String[] row : headerCsvReader.readAll()) {
					headerList.add(row[0]);
				}
				headerList.add(0, "WEL_V2/ID");
				headerCsvReader.close();
			}
		}
//		System.out.println(handleIDList);
//		System.out.println(headerList);
		traverse(sourceFile);
		multiplexmltocsv(outputpath, count);
		long end = System.currentTimeMillis();
		System.out.println("Finished in " + (end - start) / (1000 * 60) + "m");

	}

	private static void traverse(File sourceFile) throws IOException {
		// TODO Auto-generated method stub
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(sourceFile);
		reader.init();
		CompressedDataItem item;
		try {
			if (!headerList.isEmpty() && handleIDList.isEmpty()) {

				while ((item = reader.next()) != null) {
					String filenameString = new String(item.getEntryName());
					String parentString = filenameString.substring(1, filenameString.lastIndexOf("/"));
//					System.out.println(p.equals(parentString));
					
					if (item.getEntryName().endsWith(".xml")) {
						String fileContent = new String(item.getContents());
						parseDublin(fileContent);
						if (!p.equals(parentString)) {
							p = parentString;
							System.out.println("Accenssing File :" + parentString);
							if (!dataMap.isEmpty()) {
								setrowSet(dataMap);
							}
							dataMap.clear();
						}
					}
					if (item.getEntryName().endsWith("handle")) {
						String handleID = new String(item.getContents());
						dataMap.put("handleId", new ArrayList<String>() {
							{
								add(handleID);
							}
						});
					}
				}
				for (int count = 0; count < rowSet.size(); count++) {
					if (count == high) {
						multiplexmltocsv(outputpath, count);
						low = high;
						high = low + thresold;
					}
					break;
				}
			}

			//////////////////////////////////////////////
			while ((item = reader.next()) != null) {
				if (item.getEntryName().endsWith("handle")) {
					String handleID = new String(item.getContents());
					if (headerList.isEmpty() && !handleIDList.isEmpty() && handleIDList.contains(handleID)) {
						String filenameString = new String(item.getEntryName());
						String parentString = filenameString.substring(1, filenameString.lastIndexOf("/"));
//						System.out.println(p.equals(parentString));
						if (!p.equals(parentString)) {
							p = parentString;
							System.out.println("Accenssing File :" + parentString);
							if (!dataMap.isEmpty()) {
								setrowSet(dataMap);
							}
							dataMap.clear();
						}
						if (item.getEntryName().endsWith(".xml")) {
							String fileContent = new String(item.getContents());
							parseDublin(fileContent);
						}
						if (item.getEntryName().endsWith("handle")) {
							String handleID1 = new String(item.getContents());
							dataMap.put("handleId", new ArrayList<String>() {
								{
									add(handleID1);
								}
							});
						}
					}
					for (int count = 0; count < rowSet.size(); count++) {
						if (count == high) {
							multiplexmltocsv2(outputpath, count);
							low = high;
							high = low + thresold;
						}
//						System.out.println();
						break;
					}

				}
				//

			}

		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	private static void multiplexmltocsv2(String outputpath, int count) {
		// TODO Auto-generated method stub

		String pathName=outputpath;
		String csvName = outputpath+count+".csv";
		System.out.println("path name"+csvName);
		File csvFile = new File(csvName);
		if (!csvFile.getParentFile().exists())
			csvFile.getParentFile().mkdirs();
		String[] header; 

		header = new String[nodeindexMap.size()]; 		
		for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
			//		    System.out.println(entry.getKey()+" : "+entry.getValue());
			header[entry.getValue()]=entry.getKey();

		}
		try {
			
			FileWriter fW = new FileWriter(csvFile);
			CSVPrinter csvPrinter = new CSVPrinter(fW, CSVFormat.DEFAULT.withHeader(header));
			for (String[] onerow : rowSet) {
				csvPrinter.printRecord(Arrays.asList(onerow));
			}
			
			csvPrinter.close();
			rowSet.clear();
		}
		catch(Exception e){
			e.printStackTrace();
		}    	
	
		
	}

	private static void multiplexmltocsv(String outputpath, int count) {
		String pathName = outputpath;
		String csvName = outputpath + count + ".csv";
		System.out.println("path name" + csvName);
		File csvFile = new File(csvName);
		if (!csvFile.getParentFile().exists())
			csvFile.getParentFile().mkdirs();

		String[] header = new String[nodeindexMap.size()];
		Integer[] subheader = new Integer[headerList.size()];
		for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
			header[entry.getValue()] = entry.getKey();
		}
		// Initialze subheader value
		for (int i = 0; i < headerList.size(); i++) {
			int columnvalue = 0;
			for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
				if (headerList.get(i).toString().equals(entry.getKey().toString())) {
					columnvalue = entry.getValue();
				}
			}
			subheader[i] = columnvalue;
			// System.out.println(i + "==>" + subheader[i]);
		}
		// copy value from original csv to subset of csv
		for (String[] onerow : rowSet) {
			try {
				String[] newonerow = new String[headerList.size()];

				for (int k = 0; k < headerList.size(); k++) {
					newonerow[k] = onerow[subheader[k]];
				}
				subrowSet.add(newonerow);

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
		}

		try {
			FileWriter fW = new FileWriter(csvFile);
			if (headerList.size() > 0) {
				CSVPrinter csvPrinter = new CSVPrinter(fW,
						CSVFormat.DEFAULT.withHeader(headerList.toArray(new String[0])));
				for (String[] myonerow : subrowSet) {
					csvPrinter.printRecord(Arrays.asList(myonerow));
				}
				csvPrinter.close();
				subrowSet.clear();
			} else {
				CSVPrinter csvPrinter = new CSVPrinter(fW, CSVFormat.DEFAULT.withHeader(header));
				for (String[] onerow : rowSet) {
					csvPrinter.printRecord(Arrays.asList(onerow));
				}
				csvPrinter.close();
				rowSet.clear();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void parseDublin(String fileContent) {
		// TODO Auto-generated method stub
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);
			DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
			Document dcf = documentBuilder.parse(new InputSource(new java.io.StringReader(fileContent)));
			dcf.getDocumentElement();

//			System.out.println(fileContent);
			// Paese attributes of root nodes
			Element rootElement = dcf.getDocumentElement();
			ArrayList rootElementList = listallAttributes(rootElement);
			String attrVal = rootElementList.get(0).toString();

			NodeList nList = dcf.getDocumentElement().getChildNodes();
			nodeindexMap.put("WEL_V2/ID", 0);
			for (int i = 0; i < nList.getLength(); i++) {
				Node nNode = nList.item(i);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					ArrayList attrlist = listallAttributes(eElement);
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

					if (!nodeindexMap.containsKey(columnname)) {
						nodeindexMap.put(columnname, counter++);
					}
//					System.out.println("node index map  :"+nodeindexmap);

					String textContent = eElement.getTextContent();
					if (dataMap.containsKey(columnname))
						dataMap.get(columnname).add(textContent);
					else
						dataMap.put(columnname, new ArrayList<>() {
							{
								add(textContent);
							}
						});
				}
			}

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();

		}

	}

	private static ArrayList listallAttributes(Element rootElement) {

		// get a map containing the attributes of this node
		NamedNodeMap attributes = rootElement.getAttributes();
//		System.out.println(attributes);
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

	private static void setrowSet(HashMap<String, ArrayList<String>> valueMap) {
		// TODO Auto-generated method stub

		// TODO Auto-generated method stub
		String[] row = new String[nodeindexMap.size()];
		for (Map.Entry<String, ArrayList<String>> entry : valueMap.entrySet()) {
			String columnname = entry.getKey();
			int columnindex = 0;
			if (nodeindexMap.containsKey(columnname))
				columnindex = nodeindexMap.get(columnname);
			String data = "";
			for (String eachValue : entry.getValue())
				data += eachValue + " | ";
			data = data.replaceAll("\\|\\s$", ""); // replace the last "|"
			row[columnindex] = data;
		}
		row[0] = valueMap.get("handleId").get(0);
		rowSet.add(row);

	}

}
