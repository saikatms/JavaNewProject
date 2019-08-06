package org.iitkgp.ndl.SIPtoCSV;

import java.io.File;
import java.util.Scanner;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;

import SIPConversion.FieldDatabyFilterNew;

public class Wrapper {
	/*
	 * @author Saikat
	 */

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Properties prop = allExceptionHandle(args);

			String sourcePath = prop.getProperty("source");
			String destPath = prop.getProperty("target");
			String threshold = prop.getProperty("threshold");
			Integer thresholdvalue = Integer.valueOf(threshold);
			String filter = prop.getProperty("filTer");

			String handleListPath = "";

			if (prop.containsKey("handleList"))
				handleListPath = prop.getProperty("handleList");

			String columnListPath = "";

			if (prop.containsKey("columnList"))
				columnListPath = prop.getProperty("columnList");

			String itemSelector = prop.getProperty("itemSelector");
//			String partialList=prop.getProperty("partialList");

			String LogPath = "";
			if (prop.containsKey("logPath")) {
				LogPath = prop.getProperty("logPath");
			}

			String FilterField = "";
			if (prop.containsKey("filterField")) {
				FilterField = prop.getProperty("filterField");
			}

			String FilterType = "";
			if (prop.containsKey("filterType")) {
				FilterType = prop.getProperty("filterType");
			}

			String FilterValue = "";
			if (prop.containsKey("filterValue")) {
				FilterValue = prop.getProperty("filterValue");
			}

			// for no Filter
			if (filter.contains("false")) {
				if (handleListPath.isBlank() && columnListPath.isBlank() && !sourcePath.isBlank() && !destPath.isBlank()
						&& !threshold.isBlank() && LogPath.isBlank()) {
					System.out.println("Do you want to get all dump data : Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
						SIP2CSVV4 allitems = new SIP2CSVV4(sourcePath, destPath, thresholdvalue);
						System.out.println("All items with all column");
					} else {
						System.out.println("You don't want all dump data... program terminated");
					}
				}

				/*
				 * example.. if lrmi.learningResourceType is available then only print that item
				 * in csv with its respective handle id and dc.contributer.author
				 */
				else if (!sourcePath.isBlank() && !destPath.isBlank() && !columnListPath.isBlank()
						&& handleListPath.isBlank() && !threshold.isBlank() && LogPath.isBlank()
						&& itemSelector.contains("true")) {
					System.out.println("Do you want dump data for choosen field and true item selector ?: Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
//					System.out.println("If some field choosed ... For those id whose all those field exist... then only that id willl be shown in CSV  ");

					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
						SIP2CSVChooseColumn_v2 chooseColumn = new SIP2CSVChooseColumn_v2(sourcePath, destPath,
								columnListPath, thresholdvalue);
					} else {
						System.out.println(
								"You don't want dump data for choosen field and true item selector... program terminated");
					}
				}
				// ********
				else if (!sourcePath.isBlank() && !destPath.isBlank() && !columnListPath.isBlank()
						&& handleListPath.isBlank() && !threshold.isBlank() && LogPath.isBlank()
						&& itemSelector.contains("false")) {

					System.out.println(
							"Do you want dump data for some choosen field and false item selector : Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
						SIP2CSVWithHandleID_v2 chooseColumn = new SIP2CSVWithHandleID_v2(columnListPath, sourcePath,
								destPath, thresholdvalue);
						System.out.println("Choose Column");
					} else {
						System.out.println(
								"You don't want dump data for some choosen field and false item selector... Program terminated ");
					}

				} else if (!sourcePath.isBlank() && !destPath.isBlank() && columnListPath.isBlank()
						&& !handleListPath.isBlank() && !threshold.isBlank() && LogPath.isBlank()) {
					System.out.println("Do you want all filed data for some given handle ID ?: Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
						SIP2CSVWithHandleID_v2 SubCol = new SIP2CSVWithHandleID_v2(handleListPath, sourcePath, destPath,
								thresholdvalue);
						System.out.println("Sub Col");
					} else {
						System.out.println("You don't want dump data for some handle id... Program terminated ");
					}
				} else if (!sourcePath.isBlank() && !handleListPath.isBlank() && !columnListPath.isBlank()
						&& !destPath.isBlank() && !threshold.isBlank() && itemSelector.contains("false")
						&& LogPath.isBlank()) {
					System.out.println(
							"Do you want dump data for some given Handle ID and its choosen field value where item selector is false ? Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
						SIP2CSVWithHandleID_v2 subcolsubrow = new SIP2CSVWithHandleID_v2(handleListPath, sourcePath,
								columnListPath, destPath, thresholdvalue);
					} else {
						System.out.println(
								"You don't want dump data for some Handle id and its some choosen field.,.. Program terminated");
					}

				} else if (!sourcePath.isBlank() && !handleListPath.isBlank() && !columnListPath.isBlank()
						&& !destPath.isBlank() && !threshold.isBlank() && itemSelector.contains("true")
						&& LogPath.isBlank()) {
					System.out.println(
							"Do you want dump data for some given Handle ID and its choosen field value where item selector is true ? Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
						SIP2CSVItemSelectorExtension haldleIdSelector = new SIP2CSVItemSelectorExtension(handleListPath,
								sourcePath, columnListPath, destPath, thresholdvalue);
					} else {
						System.out.println(
								"You don't want dump data for some Handle id and its some choosen field.,.. Program terminated");
					}
				}

			} else if (filter.contains("true")) {
				if (handleListPath.isBlank() && columnListPath.isBlank() && !sourcePath.isBlank() && !destPath.isBlank()
						&& !threshold.isBlank() && LogPath.isBlank()) {
					System.out.println("Do you want to get all dump data where filter applied ? Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
						SIP2CSVAllColumnAllRowWithFilter ACALWF = new SIP2CSVAllColumnAllRowWithFilter(sourcePath,
								destPath, thresholdvalue, FilterField, FilterType, FilterValue);
					} else {
						System.out.println("You don't want all dump data for applied filter... program terminated");
					}

				} else if (!sourcePath.isBlank() && !destPath.isBlank() && !columnListPath.isBlank()
						&& handleListPath.isBlank() && !threshold.isBlank() && LogPath.isBlank()
						&& itemSelector.contains("false")) {
					System.out.println(
							"Do you want dump data for some choosen field and false item selector with filter ? Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
						SIPToCSVSubColumnWithFilter subColumnWithFilter = new SIPToCSVSubColumnWithFilter(
								columnListPath, sourcePath, destPath, thresholdvalue, FilterField, FilterType,
								FilterValue);
					} else {
						System.out.println(
								"You don't want dump data for some choosen field and false item selector... Program terminated ");
					}

				} else if (!sourcePath.isBlank() && !destPath.isBlank() && columnListPath.isBlank()
						&& !handleListPath.isBlank() && !threshold.isBlank() && LogPath.isBlank()
						&& itemSelector.contains("false")) {
					System.out.println(
							"Do you want all filed data for some given handle ID  and filter is applied?: Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
						SIPToCSVSubColumnWithFilter subHandleWithFilter = new SIPToCSVSubColumnWithFilter(
								handleListPath, sourcePath, destPath, thresholdvalue, FilterField, FilterType,
								FilterValue);
					} else {
						System.out.println("You don't want dump data for some handle id ... Program terminated ");
					}

				} else if (!sourcePath.isBlank() && !destPath.isBlank() && !columnListPath.isBlank()
						&& handleListPath.isBlank() && !threshold.isBlank() && LogPath.isBlank()
						&& itemSelector.contains("true")) {
					System.out.println(
							"Do you want dump data for some choosen field value where item selector is true and filter ios applied? Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
					SIPTOCSVCsooseColumnWIthFilterItemSelector ChooseColumnItemSelector = new SIPTOCSVCsooseColumnWIthFilterItemSelector(
							sourcePath, destPath, columnListPath, thresholdvalue, FilterField, FilterType, FilterValue);
					}
					else
					{
						System.out.println("Program Terminated");
					}
				} else if (!sourcePath.isBlank() && !handleListPath.isBlank() && !columnListPath.isBlank()
						&& !destPath.isBlank() && !threshold.isBlank() && itemSelector.contains("false")
						&& LogPath.isBlank()) {
					System.out.println(
							"Do you want dump data for some given Handle ID and its choosen field value where item selector is false and filter applied ? Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
						SIPTOCSVSubColSubRowWithFilter subColSubRowFilter = new SIPTOCSVSubColSubRowWithFilter(
								handleListPath, sourcePath, columnListPath, destPath, thresholdvalue, FilterField,
								FilterType, FilterValue);
					}
					else
					{
						System.out.println("program terminated");
					}
				} else if (!sourcePath.isBlank() && !handleListPath.isBlank() && !columnListPath.isBlank()
						&& !destPath.isBlank() && !threshold.isBlank() && itemSelector.contains("true")
						&& LogPath.isBlank()) {
					System.out.println(
							"Do you want dump data for some given Handle ID and its choosen field value where item selector is true  and filter applied? Choose(Y/N)");
					Scanner yesNoScan = new Scanner(System.in);
					String yesNo = yesNoScan.next();
					if (yesNo.equalsIgnoreCase("y") || yesNo.equalsIgnoreCase("yes")) {
					SIPTOCSVSubColSubRowWithFilterAndItemSelector scsrFilterAndItemselector = new SIPTOCSVSubColSubRowWithFilterAndItemSelector(
							handleListPath, sourcePath, columnListPath, destPath, thresholdvalue, FilterField,
							FilterType, FilterValue);
					}
					else
					{
						System.out.println("program terminated");
					}
				}

			}
			else
			{
				System.out.println("Program terminated due to invalid input format");
			}

			// This is for analyzer
//			else if (!sourcePath.isBlank() && !LogPath.isBlank() && !FilterField.isBlank() && !FilterType.isBlank()
//					&& !FilterValue.isBlank() && !ContinuousMatch.isBlank() && !Lftokenizer.isBlank()
//					&& !rttokenizer.isBlank() && !IgnoreCase.isBlank()) {
//				FieldDataByFilter filterr = new FieldDataByFilter(sourcePath, LogPath, FilterField, FilterType,
//						FilterValue, ListPath, ContinuousMatch, Lftokenizer, rttokenizer, IgnoreCase);
//			}

		}

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
