package org.iitkgp.ndl.SIPtoCSV.bkp;

import java.io.File;
import java.io.FileNotFoundException;
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
import org.apache.commons.lang3.text.StrBuilder;
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

public class SIP2CSVChooseColumn_v1 {

	public static int count = 0;
	static int counter = 1;
	private static Map<String, Integer> nodeindexmap = new HashMap<String, Integer>();
	private static HashMap<String, ArrayList<String>> dataMap = new HashMap<String, ArrayList<String>>();
	private static Set<String[]> rowset = new HashSet<String[]>();
	private static Set<String[]> subrowSet = new HashSet<String[]>();
	private static List<String> headerList = new ArrayList<String>();
	private static String outputPath = "";
	private static int thresold = 50000;
	private static int low = 0;
	private static int high = thresold;
	public static String p = "";

	public SIP2CSVChooseColumn_v1(String sourcePath, String destPath, String columnList, int thresold)
			throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Choosen Column Program Started...Please Wait till the end is up...");
		long start = System.currentTimeMillis();
		File sourceFile = new File(sourcePath);
		outputPath = destPath;
		CSVReader headerCSV = new CSVReader(new FileReader(new File(columnList)));
		for (String[] row : headerCSV.readAll()) {
			headerList.add(row[0]);
		}
		headerList.add(0, "WEL_V2/ID");
		headerCSV.close();
		traverse(sourceFile);
		multiplexmltocsv(outputPath, count);
		long end = System.currentTimeMillis();
		System.out.println("Finished in " + (end - start) / (1000 * 60) + "m");

	}

	void traverse(File sourceFile) throws IOException {
		// TODO Auto-generated method stub
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(sourceFile);
		reader.init();
		CompressedDataItem item;
		try {
			while ((item = reader.next()) != null) {
				String filenameString = new String(item.getEntryName());
				String parentString = filenameString.substring(0, filenameString.lastIndexOf("/"));
				if (item.getEntryName().endsWith(".xml")) {
					String fileContents = new String(item.getContents());
					// parsing filecontent of xml file

					parseDublin(fileContents);
					if (!p.equals(parentString)) {
						p = parentString;
						System.out.println("Accessing File : " + count + " : " + parentString);
						if (!dataMap.isEmpty())
							setrowSet(dataMap);
						dataMap.clear();
						count++;
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
			for (int count = 0; count < rowset.size(); count++) {
				if (count == high) {
					multiplexmltocsv(outputPath, count);
					low = high;
					high = low + thresold;
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	void multiplexmltocsv(String outputPath, int count) {
		// TODO Auto-generated method stub
		String Path = outputPath;
		String csvName = Path + count + ".csv";
		System.out.println("out put path :" + csvName);

		File csvFile = new File(csvName);
		String[] header = new String[nodeindexmap.size()];
		Integer[] subheader = new Integer[headerList.size()];
		for (Map.Entry<String, Integer> entry : nodeindexmap.entrySet()) {
			header[entry.getValue()] = entry.getKey();
		}
		for (int i = 0; i < headerList.size(); i++) {
			int columnvalue = 0;
			for (Map.Entry<String, Integer> entry : nodeindexmap.entrySet()) {
				if (headerList.get(i).toString().equals(entry.getKey().toString())) {
					columnvalue = entry.getValue();
				}
			}
			subheader[i] = columnvalue;
//				System.out.println(i + "==>" + subheader[i]);
		}
//			 copy value from original csv to subset of csv
		for (String[] onerow : rowset) {
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
					boolean empty = false;
					for (int i = 0; i < myonerow.length; i++) {
						if (myonerow[i] == null) {
							empty = true;
						}
					}
					if (empty != true) {
						csvPrinter.printRecord(Arrays.asList(myonerow));
					}
				}
				csvPrinter.close();
				subrowSet.clear();
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

	void parseDublin(String fileContentString) {
		// TODO Auto-generated method stub
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);
			DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
			Document dcf = documentBuilder.parse(new InputSource(new java.io.StringReader(fileContentString)));
			dcf.getDocumentElement();

			// Paese attributes of root nodes
			Element rootElement = dcf.getDocumentElement();
			ArrayList rootElementList = listallAttributes(rootElement);
			String attrVal = rootElementList.get(0).toString();

			NodeList nList = dcf.getDocumentElement().getChildNodes();
			nodeindexmap.put("WEL_V2/ID", 0);
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
					if (!nodeindexmap.containsKey(columnname)) {
						nodeindexmap.put(columnname, counter++);
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

	private static void setrowSet(HashMap<String, ArrayList<String>> valueMap) {
		// TODO Auto-generated method stub
		String[] row = new String[nodeindexmap.size()];
//		System.out.println("row :" + row);
		for (Map.Entry<String, ArrayList<String>> entry : valueMap.entrySet()) {
			String columnname = entry.getKey();
			int columnindex = 0;
			if (nodeindexmap.containsKey(columnname))
				columnindex = nodeindexmap.get(columnname);
			String data = "";
			for (String eachValue : entry.getValue())
				data += eachValue + " | ";
			data = data.replaceAll("\\|\\s$", ""); // replace the last "|"
			row[columnindex] = data;
		}
		row[0] = valueMap.get("handleId").get(0);
		rowset.add(row);

	}

}
