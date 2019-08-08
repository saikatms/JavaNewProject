package org.iitkgp.ndl.SIPtoCSV;

import java.io.File;
import java.io.FileNotFoundException;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class SIPTOCSVCsooseColumnWIthFilterItemSelector {
	public static int count = 0;
	static int counter = 1;
	private static Map<String, Integer> nodeindexmap = new HashMap<String, Integer>();
	private static HashMap<String, ArrayList<String>> dataMap = new HashMap<String, ArrayList<String>>();
	private static Set<String[]> rowset = new HashSet<String[]>();
	private static Set<String[]> subrowSet = new HashSet<String[]>();
	private static Set<String[]> subrowSet2 = new HashSet<String[]>();
	private static Set<String[]> subrowSet3 = new HashSet<String[]>();

	public static String filterfield = "";
	public static String filtertype = "";
	public static String filtervalue = "";
	private static List<String> headerList = new ArrayList<String>();
	private static String outputPath = "";
	private static int threShold = 0;
//	private static int low = 0;
//	private static int high = thresold;
	public static String p = "";

	public SIPTOCSVCsooseColumnWIthFilterItemSelector(String sourcePath, String destPath, String columnListPath,
			Integer thresholdvalue, String filterField, String filterType, String filterValue) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Choosen Column Program Started...Please Wait till the end is up...");
		long startTime = System.nanoTime();
		File sourceFile = new File(sourcePath);
		outputPath = destPath;
		threShold = thresholdvalue;
		filterfield = filterField;
		filtertype = filterType;
		filtervalue = filterValue;
		CSVReader headerCSV = new CSVReader(new FileReader(new File(columnListPath)));
		for (String[] row : headerCSV.readAll()) {
			headerList.add(row[0]);
		}
		headerList.add(0, "WEL_V2/ID");
		headerCSV.close();
		traverse(sourceFile);
		long endTime = System.nanoTime();
		System.out.println("Finished in :" + ((endTime - startTime) / 1000000000) / 60 + " Minute");
	}

	private static boolean isContain(String source, String subItem) {
		String pattern = "\\b" + subItem + "\\b";
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(source);
		return m.find();
	}

	void traverse(File sourceFile) throws IOException {
		// TODO Auto-generated method stub
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(sourceFile);
		reader.init();
		CompressedDataItem item;
		try {
			if (isContain(filtertype, "value")) {
				while ((item = reader.next()) != null) {
					String filenameString = new String(item.getEntryName());
					String parentString = filenameString.substring(0, filenameString.lastIndexOf("/"));

					if (item.getEntryName().endsWith(".xml")) {
						String fileContent = new String(item.getContents());
						parseDublin(fileContent);
					}
					if (!p.equals(parentString)) {
						p = parentString;
						System.out.println("Accessing File : " + count + " : " + parentString);
						if (!dataMap.isEmpty()) {
							for (Map.Entry mapElement : dataMap.entrySet()) {
								String key = (String) mapElement.getKey();
								ArrayList<String> value = (ArrayList) mapElement.getValue();
								if (key.equalsIgnoreCase(filterfield)) {
									for (String eachValue : value) {
										if (eachValue.equalsIgnoreCase(filtervalue)) {
											setrowSet(dataMap);
											break;
										}
									}
								}
							}
						}
						dataMap.clear();
						count++;
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
				}
//						 copy value from original csv to subset of csv
				for (String[] onerow : rowset) {
					try {
						String[] newonerow = new String[headerList.size()];
						for (int k = 0; k < headerList.size(); k++) {
							if (subheader[k] >= onerow.length) {
								newonerow[k] = "";
							} else {
								newonerow[k] = onerow[subheader[k]];
							}
						}
						subrowSet.add(newonerow);
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(0);
					}
				}
				for (String[] subRow : subrowSet) {
					boolean emptyCheck = false;
					for (int i = 0; i < subRow.length; i++) {
						if (subRow[i] == null) {
							emptyCheck = true;
						}
					}

					if (emptyCheck != true) {
						subrowSet2.add(subRow);
					}

				}

				rowsetiterator(subrowSet2);

			} else if (isContain(filtertype, "valuelist")) {
				filtervalue = filtervalue.replaceAll("^\"|\"$", "");
				ArrayList<String> arr_filterValue = new ArrayList<String>();
				String[] arr_str=filtervalue.split("\",\"");
				for (String arr_val : arr_str) {
					arr_filterValue.add(arr_val);
				}

				while ((item = reader.next()) != null) {
					String filenameString = new String(item.getEntryName());
					String parentString = filenameString.substring(0, filenameString.lastIndexOf("/"));

					if (item.getEntryName().endsWith(".xml")) {
						String fileContent = new String(item.getContents());
						parseDublin(fileContent);
					}
					if (!p.equals(parentString)) {
						p = parentString;
						System.out.println("Accessing File : " + count + " : " + parentString);
						if (!dataMap.isEmpty()) {
							for (Map.Entry mapElement : dataMap.entrySet()) {
								String key = (String) mapElement.getKey();
								ArrayList<String> value = (ArrayList) mapElement.getValue();
								if (key.equalsIgnoreCase(filterfield)) {
									for (String field_value : value) {
										for (String arr_val : arr_filterValue) {											
											if (arr_val.equalsIgnoreCase(field_value)) {
												setrowSet(dataMap);
												break;
											}
										}
									}

								}
							}
						}
						dataMap.clear();
						count++;
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
				}
//						 copy value from original csv to subset of csv
				for (String[] onerow : rowset) {
					try {
						String[] newonerow = new String[headerList.size()];
						for (int k = 0; k < headerList.size(); k++) {
							if (subheader[k] >= onerow.length) {
								newonerow[k] = "";
							} else {
								newonerow[k] = onerow[subheader[k]];
							}
						}
						subrowSet.add(newonerow);
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(0);
					}
				}
				for (String[] subRow : subrowSet) {
					boolean emptyCheck = false;
					for (int i = 0; i < subRow.length; i++) {
						if (subRow[i] == null) {
							emptyCheck = true;
						}
					}

					if (emptyCheck != true) {
						subrowSet2.add(subRow);
					}
				}

				rowsetiterator(subrowSet2);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	private void rowsetiterator(Set<String[]> subrowSet2) {
		// TODO Auto-generated method stub
		int x = 1;
		System.out.println(threShold);

		if (subrowSet2.size() % threShold == 0) {
			for (String[] strings : subrowSet2) {
				subrowSet3.add(strings);
				if (x % threShold == 0) {
					multiplexmltocsv(outputPath, x);
					subrowSet3.clear();
				}
				x++;
			}
		} else {
			for (String[] strings : subrowSet2) {
				subrowSet3.add(strings);
				if (x % threShold == 0) {
					multiplexmltocsv(outputPath, x);
					subrowSet3.clear();
				}
				x++;
			}
			multiplexmltocsv(outputPath, --x);
		}
	}

	void multiplexmltocsv(String outputPath, int count) {
		// TODO Auto-generated method stub
		String Path = outputPath;
		String csvName = Path + count + ".csv";
		System.out.println("out put path :" + csvName);

		File csvFile = new File(csvName);
		String[] header = new String[nodeindexmap.size()];

		try {
			FileWriter fW = new FileWriter(csvFile);
			if (headerList.size() > 0) {
				CSVPrinter csvPrinter = new CSVPrinter(fW,
						CSVFormat.DEFAULT.withHeader(headerList.toArray(new String[0])));
				for (String[] myonerow : subrowSet3) {
					csvPrinter.printRecord(Arrays.asList(myonerow));

				}
				csvPrinter.close();
				subrowSet3.clear();
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
//			System.out.println("row :" + row);
		for (Map.Entry<String, ArrayList<String>> entry : valueMap.entrySet()) {
			String columnname = entry.getKey();
			int columnindex = 0;
			if (nodeindexmap.containsKey(columnname))
				columnindex = nodeindexmap.get(columnname);
			String data = "";
			for (String eachValue : entry.getValue())
				data += eachValue + "|";
//				data = data.replaceAll("\\|\\s$", ""); // replace the last "|"
			data = data.replaceAll("\\|$", "");// replace the last "|"
			data = data.trim();

			row[columnindex] = data;
		}
		row[0] = valueMap.get("handleId").get(0);
		rowset.add(row);

	}

}
