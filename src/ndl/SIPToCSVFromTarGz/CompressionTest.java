package ndl.SIPToCSVFromTarGz;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public class CompressionTest {
	static int count;
	static int counter = 1;
	private static Map<String, Integer> nodeindexmap = new HashMap<String, Integer>();
	private static Set<String[]> rowset = new HashSet<String[]>();
	private static String outputPath = "";
	private static int thresold = 100;
	private static int low = 0;
	private static int high = thresold;
	public static String[] row;

	public static void main(String[] args) throws Exception {

		System.out.println("Program started ...");
		long start = System.currentTimeMillis();
		if (args.length != 2)
			throw new ArithmeticException(
					"Wrong Input Argument. Command line should be provide source path and destination path");
		if (args[0].isEmpty() | args[1].isEmpty()) {
			throw new NullPointerException("Please provide valid data");
		}
		File source = new File(args[0]);
		outputPath = args[1];
		if (source.exists()) {
			traverse(source);
		}
		multiplexmltocsv(outputPath, count);
		long end = System.currentTimeMillis();
		System.out.println("Finished in " + (end - start) / (1000 * 60) + "m");

	}

	public static void traverse(File parent) throws IOException {
//		System.out.println("Path of parent folder :"+parent.getAbsolutePath());
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(parent);
		reader.init();
		CompressedDataItem item;
		String HandleID = null;
		try {
			String fileContents = null;
			while ((item = reader.next()) != null) {
				String s1 = new String(item.getEntryName());
				String[] words = s1.split("/");

//				System.out.println("Words :" + words[1]);
				System.out.println("Each file :"+item.getEntryName());
				
				if (item.getEntryName().endsWith(".xml")) {
					count++;
//					System.out.println("Entry Name :"+item.getEntryName());
//					System.out.println("Accessing File :" + count + item.getEntryName());
					fileContents = new String(item.getContents());
					parseDublin(fileContents);
					setvalueMap(fileContents, HandleID);
				}
//				System.exit(0);
//				if (item.getEntryName().endsWith("/dublin_core.xml")) {
//					count++;
//					System.out.println("Accessing File :" + count + item.getEntryName());
//					fileContents = new String(item.getContents());
//					parseDublin(fileContents);
//					setvalueMap(fileContents, HandleID);
//				}
//				if (item.getEntryName().endsWith("metadata_lrmi.xml")) {
////					count++;
////					System.out.println("Accessing File :" + count + item.getEntryName());
//					fileContents = new String(item.getContents());
//					parseDublin(fileContents);
//					setvalueMap(fileContents, HandleID);
//
//				}
				if (item.getEntryName().endsWith("/handle")) {
					HandleID = new String(item.getContents());
//					System.out.println(HandleID);
				}
				if (count == high) {
					multiplexmltocsv(outputPath, count);
					low = high;
					high = low + thresold;
				}
			}
		} catch (Exception e) {
			System.out.println(e);
		}

	}

	private static void multiplexmltocsv(String path, int count) {
		String csvName = path + count + ".csv";
//		System.out.println("CSV name: "+csvName);
		File csvFile = new File(csvName);
		if (!csvFile.getParentFile().exists())
			csvFile.getParentFile().mkdirs();
		String[] header;

		header = new String[nodeindexmap.size()];
		for (Map.Entry<String, Integer> entry : nodeindexmap.entrySet()) {
//				    System.out.println(entry.getKey()+" : "+entry.getValue());
			header[entry.getValue()] = entry.getKey();

		}
		try {
			FileWriter fW = new FileWriter(csvFile);
			CSVPrinter csvPrinter = new CSVPrinter(fW, CSVFormat.DEFAULT.withHeader(header));

			for (String[] onerow : rowset) {
				csvPrinter.printRecord(Arrays.asList(onerow));
			}
			csvPrinter.close();
			rowset.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void setvalueMap(String fileContents, String HandleID) {
		// TODO Auto-generated method stub
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder;
			dBuilder = dbFactory.newDocumentBuilder();
//			System.out.println("SVM : " + HandleID + fileContents);
			if (fileContents.contains("dublin_core schema=\"dc\"")) {
//				System.out.println("DC");
				Document doc = dBuilder.parse(new InputSource(new java.io.StringReader(fileContents)));
				doc.getDocumentElement().normalize();

				Element rootElement = doc.getDocumentElement();

				ArrayList rootElementlist = listAllAttributes(rootElement);

				String attrVal = rootElementlist.get(0).toString();

				String[] row = new String[nodeindexmap.size()];

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
//					if (row[0]==null) {
					row[0] = HandleID;

//					}
					if (row[columnindex] == null) {
						row[columnindex] = data;
					} else {
						row[columnindex] = row[columnindex] + " " + data;

					}
					rowset.add(row);
					System.out.println(columnname+"  "+row[columnindex]);

				}
			} else if(fileContents.contains("<dublin_core schema=\"lrmi\">")){
				Document doc = dBuilder.parse(new InputSource(new java.io.StringReader(fileContents)));
				doc.getDocumentElement().normalize();

				Element rootElement = doc.getDocumentElement();

				ArrayList rootElementlist = listAllAttributes(rootElement);

				String attrVal = rootElementlist.get(0).toString();

				row = new String[nodeindexmap.size()];

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
//					if (row[0] == null) {
//						row[0] = HandleID;
//
//					}
					if (row[columnindex] == null) {
						row[columnindex] = data;
					} else {
						row[columnindex] = row[columnindex] + " " + data;

					}
					rowset.add(row);
					System.out.println(columnname+"  "+row[columnindex]);

				}
			}
			

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	private static void parseDublin(String fileContents) {
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
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	private static ArrayList listAllAttributes(Element element) {

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