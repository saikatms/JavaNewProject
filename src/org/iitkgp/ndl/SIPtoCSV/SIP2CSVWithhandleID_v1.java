package org.iitkgp.ndl.SIPtoCSV;

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
	public static int count=0;
	static int counter = 1;
	private static Map<String, Integer> nodeindexMap = new HashMap<String, Integer>();
	private static HashMap<String, ArrayList<String>> dataMap = new HashMap<String, ArrayList<String>>();
	private static Set<String[]> rowSet = new HashSet<String[]>();
	private static Set<String[]> subrowSet = new HashSet<String[]>();
	private static List<String> headerList = new ArrayList<String>();
	private static List<String> handleIDList = new ArrayList<String>();

	private static String outputpath = "";
	private static int thresold = 100;
	private static int low = 0;
	private static int high = thresold;
	public static String p = "";
	int rowcount;

	public SIP2CSVWithhandleID_v1(String HandleList, String source, String columnList, String destPath)
			throws Exception {

		// TODO Auto-generated method stub
		System.out.println("Program Started...");
		long start = System.currentTimeMillis();
		String HandlePath = HandleList;
		String columnPath = columnList;
		File sourceFile = new File(source);
		outputpath = destPath;
		System.out.println("HandlePath ..." + HandlePath);
		System.out.println("columnPath .." + columnPath);
		if (HandlePath.isBlank() || columnPath.isBlank()) {
			throw new Exception("Wrong input [handle list]/[column].");
		}

		if (!HandlePath.isBlank()) {
			CSVReader handleCsvReader = new CSVReader(new FileReader(HandlePath));
			for (String[] row : handleCsvReader.readAll()) {
				handleIDList.add(row[0]);
			}
//			System.out.println("handle id list "+handleIDList);
			handleCsvReader.close();
		}
		if (!columnPath.isBlank()) {
			CSVReader headerCsvReader = new CSVReader(new FileReader(columnPath));
			for (String[] row : headerCsvReader.readAll()) {
				headerList.add(row[0]);
			}
			headerList.add(0, "WEL_V2/ID");
			headerCsvReader.close();
		}
//		System.out.println(handleIDList);
//		System.out.println(headerList);
		traverse(sourceFile);
		multiplexmltocsv(outputpath, count);
		long end = System.currentTimeMillis();
		System.out.println("Finished in " + (end - start) / (1000 * 60) + "m");

	}

	public SIP2CSVWithhandleID_v1(String columnOrHandlePath, String source, String destPath) throws Exception {
		System.out.println("Program Started...");
		long start = System.currentTimeMillis();
//		String runType = runType1;

		String columnPath = columnOrHandlePath;
		String handlePath = columnOrHandlePath;
		File sourceFile = new File(source);
		outputpath = destPath;

		if (!(columnPath.contains("column") || handlePath.contains("handle"))) {
			throw new Exception(
					"Please Rename Input Folder... For Column list input must include 'column' in it name and for Handle list input please include 'handle' in it.");
		}
		if (handlePath.contains("handle")) {
			CSVReader handleCsvReader = new CSVReader(new FileReader(handlePath));
			for (String[] row : handleCsvReader.readAll()) {
				handleIDList.add(row[0]);
			}
			// System.out.println("handle id list :"+handleIDList);
			handleCsvReader.close();
		}


		if (columnPath.contains("column")) {
			CSVReader headerCsvReader = new CSVReader(new FileReader(columnOrHandlePath));
			for (String[] row : headerCsvReader.readAll()) {
				headerList.add(row[0]);
			}
			headerList.add(0, "WEL_V2/ID");
			headerCsvReader.close();
		}

		traverse(sourceFile);
		multiplexmltocsv(outputpath, count);
		long end = System.currentTimeMillis();
		System.out.println("Finished in " + (end - start) / (1000 * 60) + "m");
	}

	void traverse(File sourceFile) throws IOException {
		// TODO Auto-generated method stub
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(sourceFile);
		reader.init();
		CompressedDataItem item;
//		int count = 0;
		try {
			if (!headerList.isEmpty() && handleIDList.isEmpty()) {
				// Choose Column for all id
				while ((item = reader.next()) != null) {
					String filenameString = new String(item.getEntryName());
					String parentString = filenameString.substring(1, filenameString.lastIndexOf("/"));

					if (item.getEntryName().endsWith(".xml")) {
						String fileContent = new String(item.getContents());
						parseDublin(fileContent);
					}

					if (!p.equals(parentString)) {
						p = parentString;
						System.out.println("Accenssing Item : " + count++ + " : " + parentString);
						if (!dataMap.isEmpty()) {
							setrowSet(dataMap);
						}
						dataMap.clear();
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

				if (!dataMap.isEmpty())
					setrowSet(dataMap);

				for (int rowcount = 0; rowcount < rowSet.size(); rowcount++) {
					if (count == high) {
						multiplexmltocsv(outputpath, rowcount);
						low = high;
						high = low + thresold;
					}
				}
			}

			//////// Sub Items all Column//////////
			if (headerList.isEmpty() && !handleIDList.isEmpty()) {
				int item_count=0;
				while ((item = reader.next()) != null) {
					String s1 = new String(item.getEntryName());
					String parentString = s1.substring(0, s1.lastIndexOf("/"));

					if (!p.equals(parentString)) {
						p = parentString;
						System.out.println("Accenssing Item : " + item_count++ + " : " + parentString);

						if (!dataMap.isEmpty() && handleIDList.contains(dataMap.get("handleId").get(0))) {
							setrowSet(dataMap);
							count++;
						}
						dataMap.clear();
					}
					if (item.getEntryName().endsWith(".xml")) {
						String fileContent = new String(item.getContents());
						parseDublin(fileContent);
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
				for (int rowcount = 0; rowcount < rowSet.size(); rowcount++) {
					if (count == high) {
						multiplexmltocsv2(outputpath, rowcount);
						low = high;
						high = low + thresold;
					}
				}
			}

			// column list for given handleid

			if (!headerList.isEmpty() && !handleIDList.isEmpty()) {
				System.out.println("Here we go");
				int item_count=0;
				while ((item = reader.next()) != null) {
					String filnameString = new String(item.getEntryName());
					String parentString = filnameString.substring(0, filnameString.lastIndexOf("/"));
					if (!p.equals(parentString)) {
						p = parentString;
						System.out.println("Accenssing Item : " + item_count++ + " : " + parentString);
						if (!dataMap.isEmpty() && handleIDList.contains(dataMap.get("handleId").get(0))) {
//							System.out.println("Accenssing Item : " + count++ + " : " + parentString);
							count++;
							setrowSet(dataMap);
						}
						dataMap.clear();
					}
					if (item.getEntryName().endsWith(".xml")) {
						String fileContent = new String(item.getContents());
						parseDublin(fileContent);
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

				for (int rowcount = 0; rowcount < rowSet.size(); rowcount++) {
					if (rowcount == high) {
						multiplexmltocsv(outputpath, rowcount);
						low = high;
						high = low + thresold;
					}
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	void multiplexmltocsv2(String outputpath, int rowcount) {
		// TODO Auto-generated method stub
//System.out.println("Row count sdsfsd "+rowcount);
		String pathName = outputpath;
		String csvName = outputpath + rowcount + ".csv";
		System.out.println("path name" + csvName);
		File csvFile = new File(csvName);
		if (!csvFile.getParentFile().exists())
			csvFile.getParentFile().mkdirs();
		String[] header;

		header = new String[nodeindexMap.size()];
		for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
			header[entry.getValue()] = entry.getKey();
		}
		try {
			FileWriter fW = new FileWriter(csvFile);
			CSVPrinter csvPrinter = new CSVPrinter(fW, CSVFormat.DEFAULT.withHeader(header));
			for (String[] onerow : rowSet) {
				csvPrinter.printRecord(Arrays.asList(onerow));
			}
			csvPrinter.close();
			rowSet.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void multiplexmltocsv(String outputpath, int count) {
		String pathName = outputpath;
		String csvName = outputpath + count + ".csv";
		System.out.println("path name" + csvName);
		File csvFile = new File(csvName);
//		if (!csvFile.getParentFile().exists())
//			csvFile.getParentFile().mkdirs();

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

	void parseDublin(String fileContent) {
		// TODO Auto-generated method stub
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);
			DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
			Document dcf = documentBuilder.parse(new InputSource(new java.io.StringReader(fileContent)));
			dcf.getDocumentElement();

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

	ArrayList listallAttributes(Element rootElement) {
		// get a map containing the attributes of this node
		NamedNodeMap attributes = rootElement.getAttributes();
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

	void setrowSet(HashMap<String, ArrayList<String>> valueMap) {
		// TODO Auto-generated method stub

		String[] row = new String[nodeindexMap.size()];

		for (Map.Entry<String, ArrayList<String>> entry : valueMap.entrySet()) {
			String columnname = entry.getKey();
			int columnindex = 0;
			if (nodeindexMap.containsKey(columnname))
				columnindex = nodeindexMap.get(columnname);

			String data = "";
			for (String eachValue : entry.getValue()) {
				data += eachValue + " | ";
			}
			data = data.replaceAll("\\|\\s$", ""); // replace the last "|"
			row[columnindex] = data;
		}
		row[0] = valueMap.get("handleId").get(0);
		rowSet.add(row);
	}

}
