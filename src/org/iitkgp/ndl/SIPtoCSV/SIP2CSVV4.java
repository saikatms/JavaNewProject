package org.iitkgp.ndl.SIPtoCSV;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.FileWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.opencsv.CSVReader;

import org.iitkgp.ndl.data.compress.CompressedDataItem;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileReader;

public class SIP2CSVV4 {
	static int count;
	static int counter = 1;
	private static Map<String, Integer> nodeindexmap = new HashMap<String, Integer>();
	private static HashMap<String, ArrayList<String>> dataMap = new HashMap<String, ArrayList<String>>();
	private static Set<String[]> rowset = new HashSet<String[]>();
	private static Set<String[]> subrowset = new HashSet<String[]>();
	private static int threSholdValue = 0;
	private static String outputPath = "";
	public static String[] row;
	public static String p = "";

	public SIP2CSVV4(String sourcePath, String destPath, int threshold) throws Exception {
		System.out.println("Program started ...");
		long startTime=System.nanoTime();
		File source = new File(sourcePath);
		outputPath = destPath;
		threSholdValue = threshold;
		if (source.exists()) {
			traverse(source);
		}
		System.out.println("CSV file saved in :" + outputPath + "folder");
		long endTime = System.nanoTime();
		System.out.println("Finished in : "+((endTime-startTime)/1000000000)/60+" Minute");
	}

	void traverse(File parent) throws IOException {
		// iterating file from tar.gz file
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(parent);
		reader.init();
		CompressedDataItem item;
		try {
			while ((item = reader.next()) != null) {
				String s1 = new String(item.getEntryName());
				String parentString = s1.substring(0, s1.lastIndexOf("/"));

				if (item.getEntryName().endsWith(".xml")) {
					String fileContents = new String(item.getContents());
					// parsing filecontent of XML file
					parseDublin(fileContents);

				}
				if (!p.equals(parentString)) {
					p = parentString;
					System.out.println("Accessing File : " + count++ + " : " + parentString);

					if (!dataMap.isEmpty()) {
						setrowSet(dataMap);
					}
					dataMap.clear();
				}
				if (item.getEntryName().endsWith("handle")) {
					String HandleID = new String(item.getContents());
					// adding Handle ID in the ArrayList
					dataMap.put("handleId", new ArrayList<>() {
						{
							add(HandleID);
						}
					});
				}
			}

			if (!dataMap.isEmpty())
				setrowSet(dataMap);

			rowsetiterator(rowset);

		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private void rowsetiterator(Set<String[]> rowset2) {
		// TODO Auto-generated method stub
		int x = 1;
		if (rowset2.size() % threSholdValue == 0) {
			for (String[] strings : rowset2) {
				subrowset.add(strings);
				if (x % threSholdValue == 0) {
					multiplexmltocsv(outputPath, x);
					subrowset.clear();
				}
				x++;
			}
		} else {
			for (String[] strings : rowset2) {
				subrowset.add(strings);
				if (x % threSholdValue == 0) {
					multiplexmltocsv(outputPath, x);
					subrowset.clear();
				}
				x++;
			}
			multiplexmltocsv(outputPath, --x);
		}
	}

	void multiplexmltocsv(String path, int count) {

		String csvName = path + count + ".csv";
		System.out.println("CSV name: " + csvName);
		File csvFile = new File(csvName);
		String[] header;
		header = new String[nodeindexmap.size()];
		for (Map.Entry<String, Integer> entry : nodeindexmap.entrySet()) {
			header[entry.getValue()] = entry.getKey();
		}
		try {
			FileWriter fW = new FileWriter(csvFile);
			CSVPrinter csvPrinter = new CSVPrinter(fW, CSVFormat.DEFAULT.withHeader(header));
			for (String[] onerow : subrowset) {
				csvPrinter.printRecord(Arrays.asList(onerow));
			}
			csvPrinter.close();
			subrowset.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void setrowSet(HashMap<String, ArrayList<String>> valueMap) {
		// TODO Auto-generated method stub
		String[] row = new String[nodeindexmap.size()];
		for (Map.Entry<String, ArrayList<String>> entry : valueMap.entrySet()) {
			String columnname = entry.getKey();
			int columnindex = 0;
			if (nodeindexmap.containsKey(columnname))
				columnindex = nodeindexmap.get(columnname);
			String data = "";
			for (String eachValue : entry.getValue())
				data += eachValue + "|";

			data = data.replaceAll("\\|$", "");// replace the last "|"
			row[columnindex] = data;
		}
		row[0] = valueMap.get("handleId").get(0);
		rowset.add(row);
	}

	void parseDublin(String fileContents) {
		// TODO Auto-generated method stub
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);
			DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
			Document dcf = documentBuilder.parse(new InputSource(new java.io.StringReader(fileContents)));
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

	ArrayList listAllAttributes(Element element) {

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