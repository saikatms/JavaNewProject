package org.iitkgp.ndl.SIPtoCSV.bkp;

import java.io.File;
import java.util.Scanner;

import org.iitkgp.ndl.SIPtoCSV.SIP2CSVChooseColumn_v1;
import org.iitkgp.ndl.SIPtoCSV.SIP2CSVV4;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;

import SIPConversion.FieldDatabyFilterNew;

public class Wrapper_bkp20190626 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Properties prop = allExceptionHandle(args);

			String sourcePath = prop.getProperty("source");
			String destPath = prop.getProperty("target");

//			File handleList = null;

//			if (prop.containsKey("handleList"))
//				handleList = new File(prop.getProperty("handleList"));

			String handleList = prop.getProperty("handleList");
//			File columnList = null;
//
//			if (prop.containsKey("columnList"))
//				columnList = new File(prop.getProperty("columnList"));
			String columnList = prop.getProperty("columnList");

			Scanner in = new Scanner(System.in);
			System.out.println("Please enter choice: ");
			String choice = in.nextLine();
			int num = Integer.valueOf(choice);
			switch (num) {
			case 1:
				if (handleList == null && columnList == null) {
					SIP2CSVV4 allitems = new SIP2CSVV4(sourcePath, destPath);
				}
				break;
			case 2:
				if (columnList != null) {
					SIP2CSVChooseColumn_v1 chooseColumn = new SIP2CSVChooseColumn_v1(sourcePath, destPath, columnList);
					System.out.println("Choose Column");

				}
				break;

			default:
				break;
			}

//			FieldDatabyFilterNew dabf = new FieldDatabyFilterNew(runType);
//			dabf.fieldName = prop.getProperty("fieldName");
//			if(!runType.isEmpty() && runType.equalsIgnoreCase("-e") && !prop.getProperty("expression").isEmpty()) {			
//				dabf.expression = prop.getProperty("expression");
//			}
//			
//			if(!runType.isEmpty() && runType.equalsIgnoreCase("-l") && !prop.getProperty("listPath").isEmpty()) {
//				listPath=prop.getProperty("listPath");
//				dabf.listPath = prop.getProperty("listPath");
//				try {
//					if(prop.getProperty("continuousMatch").isBlank()) {
//						System.out.println("continuous match not given ");
//						}
//				}
//				catch(Exception e) {
//					System.out.println("please uncomment continuous match or provide it correctly");	
//					System.exit(2);
//				}
//				if (prop.getProperty("continuousMatch").equalsIgnoreCase("true"))
//					dabf.useTokenizer = false;
//				else if (prop.getProperty("continuousMatch").equalsIgnoreCase("false"))
//					dabf.useTokenizer = true;
//				else
//					throw new Exception("Use Tokenizer value wrong. Range is True/False. Program Terminating.");
//
//				if(prop.getProperty("IgnoreCase").equalsIgnoreCase("true"))
//					dabf.ic="(?i)";
//				else if (prop.getProperty("IgnoreCase").equalsIgnoreCase("false"))
//					dabf.ic="";
//				else
//					throw new Exception("Use IgnoreCase value wrong. Range is True/False. Program Terminating.");	
//
//
//				if(dabf.useTokenizer) {
//					dabf.lftokenizer = prop.getProperty("lftokenizer");
//					dabf.rttokenizer = prop.getProperty("rttokenizer");
//					try{
//						if(dabf.lftokenizer.isBlank() | dabf.rttokenizer.isBlank()) {
//							System.out.println("null value");
//						}
//					}catch(Exception e) {
//						System.out.println("As you select discrete match i.e continuousmatch =false ");
//						System.out.println("Please provide left or right tokenizer or both ");
//						System.exit(1);
//					}
//
//				}
//			}
//			dabf.source_root = prop.getProperty("source_root");
//			dabf.logFilePath = prop.getProperty("logPath");
//			dabf.pr = new PrintStream (new File (dabf.logFilePath));
//			dabf.traverse(new File (dabf.source_root));
//			dabf.pr.close();
//			System.out.println("Match Count : " + dabf.totalFieldinstances);
		}
//		
//		catch(ArithmeticException e) {
//			System.out.println(e);
//			System.exit(0);
//		}
//		catch(NullPointerException e) {
//			System.out.println("you are using expression/list  match but expression/list is not properly given in property file");
//			System.out.println("Please uncomment the expression/list from properties file or provide a valid expression/list ");
//			System.out.println("OR");
//			System.out.println("Please Provide IgnoreCase either true or false");
//			System.out.println(e);
//			System.exit(0);
//
//		}
//		catch(ArrayIndexOutOfBoundsException e) {
//			System.out.println(e);
//			System.exit(0);
//
//
//		}
//		catch(FileNotFoundException e) {
//			System.out.println("Please provide right path for properties file");
//			System.out.println(e);
//			System.exit(0);
//
//		}
		catch (Exception e) {
			System.out.println(e);
			System.out.println("you are using expression match but expression is not properly given in property file");
			System.out.println("Please uncomment the expression from properties file or provide a valid expression ");
//			if(runType.equalsIgnoreCase("-e") && prop.getProperty("expression").isEmpty()) {
//				System.out.println("RunType is -E however EXPRESSION is not specified. Program Terminating.");
//			}
		}

	}

	public static Properties allExceptionHandle(String args[]) throws Exception {
		if (args.length > 1)
			throw new ArithmeticException(
					"Wrong Input Argument. Command Syntax : java -jar FieldDatabyFilter.jar <ConfigFilePath>");

		if (args.length == 0 || args[0].isEmpty()) {
			throw new NullPointerException("Please provide valid configFile Path.");
		}

		Properties prop = new Properties();
		File properties = new File(args[0]);

		InputStream input = new FileInputStream(properties);

		prop.load(input);
		return prop;
	}

}
