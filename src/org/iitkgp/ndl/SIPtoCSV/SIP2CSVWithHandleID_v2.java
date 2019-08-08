package org.iitkgp.ndl.SIPtoCSV;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.commons.lang3.text.StrSubstitutor;
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
//import com.sun.tools.javac.code.Attribute.Array;

public class SIP2CSVWithHandleID_v2 {
	public static int count = 0;
	static int counter = 1;
	private static Map<String, Integer> nodeindexMap = new HashMap<String, Integer>();
	private static HashMap<String, ArrayList<String>> dataMap = new HashMap<String, ArrayList<String>>();
	private static Set<String[]> rowSet = new HashSet<String[]>();
	private static Set<String[]> subrowSet = new HashSet<String[]>();
	private static Set<String[]> subrowSet2 = new HashSet<String[]>();
	private static List<String> headerList = new ArrayList<String>();
	private static List<String> freshheaderList = new ArrayList<String>();
	private static List<String> handleIDList = new ArrayList<String>();

	private static String outputpath = "";
	private static int thresold = 0;
	private static int low = 0;
	private static int high = thresold;
	public static String p = "";
	int rowcount;

	public SIP2CSVWithHandleID_v2(String HandleList, String source, String columnList, String destPath,
			int threSholdvalue) throws Exception {

		// TODO Auto-generated method stub
		System.out.println("Program Started...");
		long startTime=System.nanoTime();
		String HandlePath = HandleList;
		String columnPath = columnList;
		File sourceFile = new File(source);
		outputpath = destPath;
		thresold = threSholdvalue;
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
		traverse(sourceFile);
		long endTime = System.nanoTime();
		System.out.println("Finished in :"+((endTime-startTime)/1000000000)/60+" Minute");
}

	// Sub Column of all handle id or All column for some particular handle id //

	public SIP2CSVWithHandleID_v2(String columnOrHandlePath, String source, String destPath, int threShold)
			throws Exception {
		System.out.println("Program Started...");
		long start = System.currentTimeMillis();
		String columnPath = columnOrHandlePath;
		String handlePath = columnOrHandlePath;
		File sourceFile = new File(source);
		outputpath = destPath;
		thresold = threShold;

		if (!(columnPath.contains("column") || handlePath.contains("handle"))) {
			throw new Exception(
					"Please Rename Input Folder... For Column list input must include 'column' in it name and for Handle list input please include 'handle' in it.");
		}
		if (handlePath.contains("handle")) {
			CSVReader handleCsvReader = new CSVReader(new FileReader(handlePath));
			for (String[] row : handleCsvReader.readAll()) {
				handleIDList.add(row[0]);
			}
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
		long end = System.currentTimeMillis();
		System.out.println("Finished in " + (end - start) / (1000 * 60) + "m");
	}

	// Iteration of tar.gz file operation done here
	void traverse(File sourceFile) throws IOException {
		// TODO Auto-generated method stub
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(sourceFile);
		reader.init();
		CompressedDataItem item;
		try {
			if (!headerList.isEmpty() && handleIDList.isEmpty()) {
				// Choose Column for all id
				System.out.println("Choose Column Operation Started... Please Wait");

				// Iteration of tar.gz file operation done here
				while ((item = reader.next()) != null) {
					String filenameString = new String(item.getEntryName());
					String parentString = filenameString.substring(0, filenameString.lastIndexOf("/"));
					if (item.getEntryName().endsWith(".xml")) {
						String fileContent = new String(item.getContents());
						parseDublin(fileContent); // Each Xml file is parsed
					}

					// Item under same directory the tar.gz file
					if (!p.equals(parentString)) {
						p = parentString;
						System.out.println("Accenssing Item : " + count++ + " : " + parentString);
						if (!dataMap.isEmpty()) {
							setrowSet(dataMap);
						}
						dataMap.clear();
					}

					// adding the handleid at datamap
					if (item.getEntryName().endsWith("handle")) {
						String handleID = new String(item.getContents());
						dataMap.put("handleId", new ArrayList<String>() {
							{
								add(handleID);
							}
						});
					}
				}

				// Adding last datamap in setrowset
				if (!dataMap.isEmpty())
					setrowSet(dataMap);

				String[] header = new String[nodeindexMap.size()];
				for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
					header[entry.getValue()] = entry.getKey();
				}

				for (String list : headerList) {
					for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
						if (entry.getKey().equals(list)) {
							freshheaderList.add(entry.getKey());
						}
					}
				}

				Integer[] subheader = new Integer[freshheaderList.size()];
				for (int i = 0; i < freshheaderList.size(); i++) {
					int columnvalue = 0;
					for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
						if (freshheaderList.get(i).toString().equals(entry.getKey().toString())) {
							columnvalue = entry.getValue();
						}
					}
					subheader[i] = columnvalue;
				}

				for (String[] oneRow : rowSet) {
					try {
						String[] newonerow = new String[freshheaderList.size()];
						for (int k = 0; k < freshheaderList.size(); k++) {
							if (subheader[k] >= oneRow.length) {
								newonerow[k] = "";
							} else {
								newonerow[k] = oneRow[subheader[k]];
							}
						}
						subrowSet.add(newonerow);
					} catch (Exception e) {
						// TODO: handle exception
						e.printStackTrace();
						System.exit(0);
					}
				}
				rowsetiterator(subrowSet);
			}

			//////// Sub Items all Column//////////
			if (headerList.isEmpty() && !handleIDList.isEmpty()) {
				int item_count = 0;
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
				rowsetiterator2(rowSet);
			}

			// column list for given handleid
			if (!headerList.isEmpty() && !handleIDList.isEmpty()) {
				int item_count = 0;
				while ((item = reader.next()) != null) {
					String filnameString = new String(item.getEntryName());
					String parentString = filnameString.substring(0, filnameString.lastIndexOf("/"));
					if (!p.equals(parentString)) {
						p = parentString;
						System.out.println("Accenssing Item : " + item_count++ + " : " + parentString);
						if (!dataMap.isEmpty() && handleIDList.contains(dataMap.get("handleId").get(0))) {
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

				String[] header = new String[nodeindexMap.size()];
				for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
					header[entry.getValue()] = entry.getKey();
				}
				for (String list : headerList) {
					for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
						if (entry.getKey().equals(list)) {
							freshheaderList.add(entry.getKey());
						}
					}
				}
				Integer[] subheader = new Integer[freshheaderList.size()];

				for (int i = 0; i < freshheaderList.size(); i++) {
					int columnvalue = 0;
					for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
						if (freshheaderList.get(i).toString().equals(entry.getKey().toString())) {
							columnvalue = entry.getValue();
						}
					}
					subheader[i] = columnvalue;
				}

				for (String[] oneRow : rowSet) {
					try {
						String[] newonerow = new String[headerList.size()];
						for (int k = 0; k < headerList.size(); k++) {
							if (subheader[k] >= oneRow.length) {
								newonerow[k] = "";
							} else {
								newonerow[k] = oneRow[subheader[k]];
							}
						}
						subrowSet.add(newonerow);
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(0);
					}
				}
				rowsetiterator(subrowSet);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	// Threshold operation
	private void rowsetiterator2(Set<String[]> rowSet) {
		// TODO Auto-generated method stub
		int x = 1;
		if (rowSet.size() % thresold == 0) {
			for (String[] strings : rowSet) {
				subrowSet.add(strings);
				if (x % thresold == 0) {
					multiplexmltocsv2(outputpath, x);
					subrowSet.clear();
				}
				x++;
			}
		} else {
			for (String[] strings : rowSet) {
				subrowSet.add(strings);
				if (x % thresold == 0) {
					multiplexmltocsv2(outputpath, x);
					subrowSet.clear();
				}
				x++;
			}
			multiplexmltocsv2(outputpath, --x);
		}
	}

	// Threshold operation
	private void rowsetiterator(Set<String[]> subrowSet) {
		// TODO Auto-generated method stub
		int x = 1;
		if (subrowSet.size() % thresold == 0) {
			for (String[] strings : subrowSet) {
				subrowSet2.add(strings);
				if (x % thresold == 0) {
					multiplexmltocsv(outputpath, x);
					subrowSet2.clear();
				}
				x++;
			}
		} else {
			for (String[] strings : subrowSet) {
				subrowSet2.add(strings);
				if (x % thresold == 0) {
					multiplexmltocsv(outputpath, x);
					subrowSet2.clear();
				}
				x++;
			}
			multiplexmltocsv(outputpath, --x);
		}
	}

	// Csv generation
	void multiplexmltocsv2(String outputpath, int rowcount) {
		// TODO Auto-generated method stub
		String pathName = outputpath;
		String csvName = outputpath + rowcount + ".csv";
		System.out.println("path name" + csvName);
		File csvFile = new File(csvName);

		String[] header;
		header = new String[nodeindexMap.size()];
		for (Map.Entry<String, Integer> entry : nodeindexMap.entrySet()) {
			header[entry.getValue()] = entry.getKey();
		}
		try {
			FileWriter fW = new FileWriter(csvFile);
			CSVPrinter csvPrinter = new CSVPrinter(fW, CSVFormat.DEFAULT.withHeader(header));
			for (String[] onerow : subrowSet) {
				csvPrinter.printRecord(Arrays.asList(onerow));
			}
			csvPrinter.close();
			subrowSet2.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// CSV generation
	void multiplexmltocsv(String outputpath, int count) {
		String pathName = outputpath;
		String csvName = outputpath + count + ".csv";
		System.out.println("path name" + csvName);
		File csvFile = new File(csvName);

		String[] header = new String[nodeindexMap.size()];

		try {
			FileWriter fW = new FileWriter(csvFile);
			if (freshheaderList.size() > 0) {
				CSVPrinter csvPrinter = new CSVPrinter(fW,
						CSVFormat.DEFAULT.withHeader(freshheaderList.toArray(new String[0])));
				for (String[] myonerow : subrowSet2) {
					csvPrinter.printRecord(Arrays.asList(myonerow));
				}
				csvPrinter.close();
				subrowSet2.clear();
			} else {
				CSVPrinter csvPrinter = new CSVPrinter(fW, CSVFormat.DEFAULT.withHeader(header));
				for (String[] onerow : rowSet) {
					System.out.println("one row :" + onerow);
					csvPrinter.printRecord(Arrays.asList(onerow));
				}
				csvPrinter.close();
				rowSet.clear();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// parsing Xml files
	void parseDublin(String fileContent) {
		// TODO Auto-generated method stub
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);
			DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
			Document dcf = documentBuilder.parse(new InputSource(new java.io.StringReader(fileContent)));
			dcf.getDocumentElement();

			// Parse attributes of root nodes
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
					if (dataMap.containsKey(columnname)) {
						dataMap.get(columnname).add(textContent);
					} else {
						dataMap.put(columnname, new ArrayList<>() {
							{
								add(textContent);
							}
						});
					}
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

			if (!nodeindexMap.containsKey(columnname)) {
			}
			String data = "";
			for (String eachValue : entry.getValue()) {
				data += eachValue + "|";
			}
			data = data.replaceAll("\\|$", ""); // replace the last "|"
			row[columnindex] = data;
		}
		row[0] = valueMap.get("handleId").get(0);
		rowSet.add(row);
	}

}
