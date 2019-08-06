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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.opencsv.CSVReader;

public class SIP2CSVV2 {

		// TODO Auto-generated method stub
		static int count;
		static int counter=1;
		private static Map<String, Integer> nodeindexmap = new HashMap<String,Integer>();
		private static Set<String[]> rowset = new HashSet<String[]>();
		private static String outputPath = "/home/ndl/Desktop/JavaDataCSV/CSVOUTPUT/";
		
			public static void main(String[] args) throws Exception{
				System.out.println("Program started ...");
				long start = System.currentTimeMillis();
//				String source = "/home/ndl/Desktop/dublin_core/Test_data/";
//				String source = "/home/ndl/Desktop/list1/WEL_XML_V2__TEST_OUTPUT/partI/noddc/VA/";
//				String source = "/home/ndl/Desktop/JavaDataCSV/COLL@citeseerx_batch1";
				File source = new File("/home/ndl/Desktop/JavaDataCSV/COLL@citeseerx_batch1");
				
					traverse(source);
				
				long end = System.currentTimeMillis();
				System.out.println("Finished in " + (end-start)/(1000*60) + "m");
			}
			
			public static void multiplexmltocsv(String path) {
				String csvName = path+".csv";
				System.out.println("my path"+path);
				File csvFile = new File(csvName);
				if (!csvFile.getParentFile().exists())
					csvFile.getParentFile().mkdirs();
				 String[] header; 
		         
			      header = new String[nodeindexmap.size()]; 		
			      for (Map.Entry<String, Integer> entry : nodeindexmap.entrySet()) {
//				    System.out.println(entry.getKey()+" : "+entry.getValue());
				    header[entry.getValue()]=entry.getKey();

				}
				try {
					FileWriter fW = new FileWriter(csvFile);
					CSVPrinter csvPrinter = new CSVPrinter(fW, CSVFormat.DEFAULT.withHeader(header));
					 for (String[] onerow : rowset) {
							csvPrinter.printRecord(Arrays.asList(onerow));
						 }
						csvPrinter.close();
				}
				catch(Exception e){
					e.printStackTrace();
				}    	
		    }
			public static void traverse(File parent) throws IOException{
				for(File input : parent.listFiles())
					if(input.isDirectory())
						traverse(input);
					else {
						String parent_name = parent.getAbsolutePath().replace("/home/ndl/Desktop/list1/50000_final_csv", "");
						if(count==100) {
							break;
						}
						System.out.println("Accesing File : " + count++ + " : " + parent_name);	
						parseDublin( new File(parent + "/dublin_core.xml"));
						BufferedReader brTest = new BufferedReader(new FileReader(new File(parent + "/handle")));
					    String handle = brTest .readLine();
						setvalueMap(new File(parent + "/dublin_core.xml"),handle);
//						parseDublin( new File(parent + "/metadata_lrmi.xml"));
//						setvalueMap(new File(parent + "/metadata_lrmi.xml"));
						 if (nodeindexmap != null && !nodeindexmap.isEmpty()) {

								//System.out.println("Printing CSV started");
								for (Map.Entry<String, Integer> entry : nodeindexmap.entrySet()) {
								    System.out.println(entry.getKey()+" : "+entry.getValue());
								}
								System.out.println("Printing CSV complete");
							}
						 multiplexmltocsv(outputPath);		

						break;
					}
			}
			
			
			public static void setvalueMap(File xmlFile,String handle) {
				try {
					 DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			         DocumentBuilder dBuilder;
			         dBuilder = dbFactory.newDocumentBuilder();
					Document doc = dBuilder.parse(xmlFile);
			         doc.getDocumentElement().normalize();
			         
			         Element rootElement = doc.getDocumentElement();
					   ArrayList rootElementlist= listAllAttributes(rootElement);
					    String attrVal=rootElementlist.get(0).toString();
					    
			         String[] row= new String[nodeindexmap.size()];
			         NodeList nList = doc.getDocumentElement().getChildNodes();
			         for(int j=0;j< nList.getLength();j++) {
			        	 if (nList.item(j).getNodeType() != Node.ELEMENT_NODE)
							continue;
						Element thisNode = (Element) nList.item(j);

						ArrayList attrlist=listAllAttributes(thisNode);
		    			int listSize=attrlist.size();
		    			
						String columnname="";
		    			for(int k=0;k<listSize;k++) {
		    				if(k==(listSize-1)) {
			    				columnname=columnname+attrlist.get(k);
		    				}else {
			    				columnname=columnname+attrlist.get(k)+".";
		    				}
		    			}
		    			columnname=attrVal+"."+columnname;
						
						String data= thisNode.getTextContent().trim();
						
						int columnindex = nodeindexmap.get(columnname);
						row[0]=handle;
						row[columnindex]=data;
//						System.out.println("col"+columnname+":"+data);
						rowset.add(row);
													
			         }
				}
				catch(Exception e) {
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
					
					//Parse attributes of rootnode
				    Element rootElement = dcf.getDocumentElement();
				    ArrayList rootElementlist= listAllAttributes(rootElement);
				    String attrVal=rootElementlist.get(0).toString();
		            
		            
			         NodeList nList = dcf.getDocumentElement().getChildNodes();
			         nodeindexmap.put("WEL_V2/ID", 0);
			         for (int i=0;i<nList.getLength();i++) {
			        	  Node nNode = nList.item(i);
				            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
				    			Element eElement = (Element) nNode;
				    			ArrayList attrlist=listAllAttributes(eElement);
				    			int listSize=attrlist.size();
				    			
								String columnname="";
				    			for(int k=0;k<listSize;k++) {
				    				if(k==(listSize-1)) {
					    				columnname=columnname+attrlist.get(k);
				    				}else {
					    				columnname=columnname+attrlist.get(k)+".";
				    				}
				    			}
				    			columnname=attrVal+"."+columnname;
				    			if(!nodeindexmap.containsKey(columnname)) {
				    				nodeindexmap.put(columnname, counter++);
				    			}			
				    		}
			         }

				} catch(Exception e) {
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


