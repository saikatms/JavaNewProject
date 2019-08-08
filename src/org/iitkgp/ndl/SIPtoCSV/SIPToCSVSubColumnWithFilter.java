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

public class SIPToCSVSubColumnWithFilter {
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

	public static String filterfield = "";
	public static String filtertype = "";
	public static String filtervalue = "";
	private static String outputPath = "";
	private static int thresold = 0;
	private static int low = 0;
	private static int high = thresold;
	public static String p = "";
	int rowcount;

	public SIPToCSVSubColumnWithFilter(String columnOrHandlePath, String sourcePath, String destPath,
			Integer thresholdvalue, String filterField, String filterType, String filterValue) throws Exception {
		System.out.println("Program Started...");
		long start = System.currentTimeMillis();
		String columnPath = columnOrHandlePath;
		String handlePath = columnOrHandlePath;
		File sourceFile = new File(sourcePath);
		outputPath = destPath;
		thresold = thresholdvalue;
		filterfield = filterField;
		filtertype = filterType;
		filtervalue = filterValue;
		System.out.println(columnOrHandlePath);
		if (!(columnPath.contains("column") || handlePath.contains("handle"))) {
			throw new Exception(
					"Please Rename Input filename... For Column list input must include 'column' in it name and for Handle list input please include 'handle' in it.");
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

	void traverse(File sourceFile) throws IOException {
		// TODO Auto-generated method stub
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(sourceFile);
		reader.init();
		CompressedDataItem item;
		try {
			if (isContain(filtertype, "value")) {
				// Choose Column for all id
				if (!headerList.isEmpty() && handleIDList.isEmpty()) {
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
			} else if (isContain(filtertype, "valuelist")) {
				filtervalue = filtervalue.replaceAll("^\"|\"$", "");
				ArrayList<String> arr_filterValue = new ArrayList<String>();
				String[] arr_str = filtervalue.split("\",\"");
				for (String arr_val : arr_str) {
					arr_filterValue.add(arr_val);
				}
				if (!headerList.isEmpty() && handleIDList.isEmpty()) {
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
				if (headerList.isEmpty() && !handleIDList.isEmpty()) {
					int item_count = 0;
					while ((item = reader.next()) != null) {
						String s1 = new String(item.getEntryName());
						String parentString = s1.substring(0, s1.lastIndexOf("/"));

						if (!p.equals(parentString)) {
							p = parentString;
							System.out.println("Accenssing Item : " + item_count++ + " : " + parentString);

							if (!dataMap.isEmpty() && handleIDList.contains(dataMap.get("handleId").get(0))) {
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
			}
			// column list for given handleid
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	private void rowsetiterator2(Set<String[]> rowSet) {
		// TODO Auto-generated method stub

		int x = 1;
		if (rowSet.size() % thresold == 0) {
			for (String[] strings : rowSet) {
				subrowSet.add(strings);
				if (x % thresold == 0) {
					multiplexmltocsv2(outputPath, x);
					subrowSet.clear();
				}
				x++;
			}
		} else {
			for (String[] strings : rowSet) {
				subrowSet.add(strings);
				if (x % thresold == 0) {
					multiplexmltocsv2(outputPath, x);
					subrowSet.clear();
				}
				x++;
			}
			multiplexmltocsv2(outputPath, --x);
		}
	}

	private void multiplexmltocsv2(String outputpath, int rowcount) {
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

	private void rowsetiterator(Set<String[]> subrowSet) {
		// TODO Auto-generated method stub
		int x = 1;
		if (subrowSet.size() % thresold == 0) {
			for (String[] strings : subrowSet) {
				subrowSet2.add(strings);
				if (x % thresold == 0) {
					multiplexmltocsv(outputPath, x);
					subrowSet2.clear();
				}
				x++;
			}
		} else {
			for (String[] strings : subrowSet) {
				subrowSet2.add(strings);
				if (x % thresold == 0) {
					multiplexmltocsv(outputPath, x);
					subrowSet2.clear();
				}
				x++;
			}
			multiplexmltocsv(outputPath, --x);
		}
	}

	private void multiplexmltocsv(String outputpath, int count) {
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

	private static boolean isContain(String source, String subItem) {
		String pattern = "\\b" + subItem + "\\b";
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(source);
		return m.find();
	}

}
