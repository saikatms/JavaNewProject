package org.iitkgp.ndl.SIPtoCSV.bkp;

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

public class SIP2CSVWithhandleID_v1_1 {

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
	

	public SIP2CSVWithhandleID_v1_1(String runType1, String columnOrLandleList, String sourcePath) throws Exception {
		// TODO Auto-generated constructor stub
		System.out.println("Program Started...");
		long start = System.currentTimeMillis();
		String runType = runType1;

		String columnOrHandlePath = columnOrLandleList;
		File sourceFile = new File(sourcePath);

		if (!(runType.equalsIgnoreCase("-c") || runType.equalsIgnoreCase("-l"))) {
			throw new Exception("Wrong Runtype. Runtype in -l[handle list]/-c[column].");
		}

		if (runType.equalsIgnoreCase("-l")) {
			CSVReader handleCsvReader = new CSVReader(new FileReader(columnOrHandlePath));
			for (String[] row : handleCsvReader.readAll()) {
				handleIDList.add(row[0]);
			}
//			System.out.println("handle id list :"+handleIDList);
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
		traverse(sourceFile);
//		multiplexmltocsv(outputpath, count);
		long end = System.currentTimeMillis();
		System.out.println("Finished in " + (end - start) / (1000 * 60) + "m");

//

	}

	private void traverse(File sourceFile) throws IOException {
		// TODO Auto-generated method stub
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(sourceFile);
		reader.init();
		CompressedDataItem item;
		try {
			if (headerList.isEmpty() && !handleIDList.isEmpty()) {
				String parentName = "";
				while ((item = reader.next()) != null) {
//				System.out.println("handle list :"+handleIDList);
					

					if (item.getEntryName().endsWith("handle")) {
						System.out.println("Items :"+item.getEntryName());

						String handleID = new String(item.getContents());
						if (handleIDList.contains(handleID)) {
							String HandleName = new String(item.getEntryName());
							parentName = HandleName.substring(0, HandleName.lastIndexOf("/"));
							System.out.println("Parent name :"+parentName);
//									System.out.println("handle " + item.getEntryName());
//							dataMap.put("handleId", new ArrayList<String>() {
//								{
//									add(handleID);
//								}
//							});
						}
					}
					if (item.getEntryName().endsWith(".xml")) {
						String xmlName=item.getEntryName();
						
						System.out.println("Xml Name :"+xmlName);
						System.out.println("Parent name---> :"+parentName);
					}
//					
				}
			}

		} catch (Exception e) {
			// TODO: handle exception
		}

	}

}
