package org.iitkgp.ndl.SIPtoCSV;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.iitkgp.ndl.data.compress.CompressedDataItem;
import org.iitkgp.ndl.data.compress.TarGZCompressedFileReader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.opencsv.CSVReader;

//import curation.Base;

public class FieldDataByFilter {

	String handle = "", source_root = "", logFilePath = "", fieldName = "", expression = "", runType = "",
			lftokenizer = "", rttokenizer = "", ic = "";
	static String listPath = "";
	int count = 0, totalFieldinstances = 0;
	boolean useTokenizer = true;
	ArrayList<String[]> exceptionList = new ArrayList<String[]>();
	List<String> valueList = new ArrayList<String>();
	PrintStream pr = null;

//	public static Properties allExceptionHandle(String args[]) throws ArithmeticException, NullPointerException,
//			FileNotFoundException, ArrayIndexOutOfBoundsException, Exception {
//		if (args.length != 2)
//			throw new ArithmeticException(
//					"Wrong Input Argument. Command Syntax : java -jar FieldDatabyFilter.jar -l[ist]/e[xpression] <ConfigFilePath>");
//
//		if (args[0].isEmpty() | args[1].isEmpty()) {
//			throw new NullPointerException("Please provide valid data");
//		}
//		String runType = args[0];
//
//		if (!runType.matches("(?i)-l|-e"))
//			throw new Exception("Wrong Runtype. Runtype in -l[ist]/-e[xpression].");
//		String propPath = args[1];
//		if (propPath.isEmpty())
//			throw new FileNotFoundException("Properties File not given. Please in put properties file path.");
//		Properties prop = new Properties();
//		File properties = new File(propPath);
//
//		InputStream input = new FileInputStream(properties);
//
//		prop.load(input);
//		listPath = prop.getProperty("listPath");
//		return prop;
//	}

	public FieldDataByFilter(String runType, String listPath, String filtervalue) {
		// TODO Auto-generated constructor stub
		this.runType = runType;
		System.out.println("Run type :" + runType);
		try {
			if (runType.equalsIgnoreCase("tokenlist")) {
				CSVReader cr = new CSVReader(new InputStreamReader(new FileInputStream(listPath), "UTF-8"), '|', '"');
				for (String[] row : cr.readAll()) {
					exceptionList.add(new String[] { row[0].trim(), row[1].trim() });
				}
				cr.close();
			} else if (runType.equalsIgnoreCase("regexp")) {
				expression = ".*" + filtervalue.strip() + ".*";
			} else if (runType.equalsIgnoreCase("value")) {
				expression = filtervalue.strip();
//				System.out.println("Expression :"+expression);
			} else if (runType.equalsIgnoreCase("valuelist")) {
				String str[] = filtervalue.split(",");
				valueList = Arrays.asList(str);
			}
		} catch (Exception e) {
			System.out.println("exception on loading csv");
		}
	}

	public void traverse(File input) throws Exception {
		TarGZCompressedFileReader reader = new TarGZCompressedFileReader(input);
		reader.init();
		CompressedDataItem item;
		try {
			while ((item = reader.next()) != null) {
				String s1 = new String(item.getEntryName());
				String parentString = s1.substring(0, s1.lastIndexOf("/"));

				if (item.getEntryName().endsWith(".xml")) {
					String filecontents = new String(item.getContents());
					process(filecontents);

				}
				if (item.getEntryName().endsWith("handle")) {
					handle = new String(item.getContents());
				}
			}

		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	public void process(String filecontents) throws Exception {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);

			DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
			Document inputDoc = documentBuilder.parse(new InputSource(new java.io.StringReader(filecontents)));
			inputDoc.getDocumentElement().normalize();

			Element root = inputDoc.getDocumentElement();
			String schema = root.getAttribute("schema");
			NodeList docNodes = root.getChildNodes();

			String textContent = "";
			for (int i = 0; i < docNodes.getLength(); i++) {

				Node docNode = docNodes.item(i);
				if (docNode.getNodeType() != Node.ELEMENT_NODE)
					continue;

				String nodeNameNDL = formReadable(docNode, schema);
				if (nodeNameNDL.equals(fieldName)) {
					textContent = docNode.getTextContent();
//					System.out.println("text content :"+textContent);
					if (runType.equalsIgnoreCase("tokenlist"))
						dumpByExceptionList(textContent);
					else if (runType.equalsIgnoreCase("value")) {
						dumpByExceptionValue(textContent);
					} else if (runType.equalsIgnoreCase("regexp")) {
						dumpByExceptionRegexp(textContent);
					} else if (runType.equalsIgnoreCase("valuelist")) {
						dumpByExceptionValueList(textContent);
					} else if (runType.equalsIgnoreCase("-e"))
						dumpByExceptionExpression(textContent);
					continue;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void dumpByExceptionExpression(String textContent) {
		if (textContent.matches(expression)) {
			pr.println(handle + "\t" + textContent);
			totalFieldinstances++;
		}
	}

	void dumpByExceptionValueList(String textContent) {
		
		for (String value : valueList) {
			value=value.strip();
			if (textContent.contains(value)) {
				pr.println(handle + "\t" +textContent);
				totalFieldinstances++;
			}
		}
	}

	void dumpByExceptionRegexp(String textContent) {
		if (textContent.matches(expression)) {
			pr.println(handle + "\t" + textContent);
			totalFieldinstances++;
		}
	}

	void dumpByExceptionValue(String textContent) {
		if (textContent.contains(expression)) {
			pr.println(handle + "\t" + textContent);
			totalFieldinstances++;
		}
	}

	void dumpByExceptionList(String textContent) {
		for (String[] exList : exceptionList) {
			String exChar = "";
			if (!exList[0].equalsIgnoreCase("regexp")) {
				exChar = exList[1].replaceAll("([\\W&&\\S])", "\\\\$1");
			} else {
				exChar = exList[1];
			}

			if (useTokenizer) {
				if (textContent.matches(ic + ".*" + lftokenizer + exChar + rttokenizer + ".*")) {
					pr.println(handle + "\t" + textContent);
					totalFieldinstances++;
					break;
				}
			} else {
				if (textContent.matches(ic + ".*" + exChar + ".*")) {
					pr.println(handle + "\t" + textContent);
					totalFieldinstances++;
					break;
				}
			}
		}
	}

	public String formReadable(Node thisNode, String schema) {
		// TODO Auto-generated method stub
		NamedNodeMap attrs = thisNode.getAttributes();
		String element = "", qualifier = "", read;

		for (int i = 0; i < attrs.getLength(); i++) {

			switch (attrs.item(i).getNodeName()) {
			case "element":
				element = attrs.getNamedItem("element").getNodeValue();
				break;
			case "qualifier":
				qualifier = attrs.getNamedItem("qualifier").getNodeValue();
				break;
			}
		}

		read = (schema + "." + element + "." + qualifier);
		read = read.replaceAll("[.]$", "");

		return read;
	}

	public FieldDataByFilter(String sourcePath, String logPath, String filterField, String filterType,
			String filterValue, String listPath, String continuousMatch, String lftokenizer, String rttokenizer,
			String ignoreCase) {
		try {
			String runType = filterType;
			String tokenFilePath = listPath;
//			String filterValue=
			FieldDataByFilter dabf = new FieldDataByFilter(runType, tokenFilePath, filterValue);
			dabf.fieldName = filterField;
			System.out.println("filed name :" + dabf.fieldName);

			if (!runType.isEmpty() && runType.equalsIgnoreCase("tokenlist") && !tokenFilePath.isEmpty()) {
				dabf.listPath = tokenFilePath;
				try {
					if (continuousMatch.isEmpty()) {
						System.out.println("Continuous match not given");
					}
				} catch (Exception e) {
					System.out.println("please uncomment continuous match or provide it correctly");
					System.exit(2);
				}
				if (continuousMatch.equalsIgnoreCase("true")) {
					dabf.useTokenizer = false;
				} else if (continuousMatch.equalsIgnoreCase("false")) {
					dabf.useTokenizer = true;
				} else {
					throw new Exception("Use Tokenizer value wrong. Range is True/False. Program Terminating.");
				}

				if (ignoreCase.equalsIgnoreCase("true")) {
					dabf.ic = "(?i)";
				} else if (ignoreCase.equalsIgnoreCase("false")) {
					dabf.ic = "";
				} else {
					throw new Exception("Use IgnoreCase value wrong. Range is True/False. Program Terminating.");
				}

				if (dabf.useTokenizer) {
					dabf.lftokenizer = lftokenizer;
					dabf.rttokenizer = rttokenizer;
					try {
						if (dabf.lftokenizer.isBlank() | dabf.rttokenizer.isBlank()) {
							System.out.println("null value");
						}
					} catch (Exception e) {
						// TODO: handle exception
						System.out.println("As you select discrete match i.e continuousmatch =false ");
						System.out.println("Please provide left or right tokenizer or both ");
						System.exit(1);
					}

				}
			}
			dabf.source_root = sourcePath;
			dabf.logFilePath = logPath;
			dabf.pr = new PrintStream(new File(dabf.logFilePath));
			dabf.traverse(new File(dabf.source_root));
			dabf.pr.close();
			System.out.println("Match Count : " + dabf.totalFieldinstances);

		} catch (ArithmeticException e) {
			System.out.println(e);
			System.exit(0);
		} catch (NullPointerException e) {
			System.out.println(
					"you are using expression/list  match but expression/list is not properly given in property file");
			System.out.println(
					"Please uncomment the expression/list from properties file or provide a valid expression/list ");
			System.out.println("OR");
			System.out.println("Please Provide IgnoreCase either true or false");
			System.out.println(e);
			System.exit(0);

		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println(e);
			System.exit(0);

		} catch (FileNotFoundException e) {
			System.out.println("Please provide right path for properties file");
			System.out.println(e);
			System.exit(0);

		} catch (Exception e) {
			System.out.println(e);
			System.out.println("you are using expression match but expression is not properly given in property file");
			System.out.println("Please uncomment the expression from properties file or provide a valid expression ");

		}
	}

}
