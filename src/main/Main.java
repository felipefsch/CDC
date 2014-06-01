package main;

import logBased.LogBased;
import performanceTest.PerformanceTest;
import schema.Create;
import snapshotDifferential.Differential;
import snapshotDifferential.Snapshot;
import tableScan.TableScan;
import trackingTable.PrepareTrackingTableMapRed;
import trackingTable.TrackingTableMapRed;
import utils.Utils;
import auditColumn.AuditColumn;
import auditColumn.CreateAuditColumnsMapRed;

public class Main {
	
	private static String CDC = utils.Utils.CDC;
	
	public static void printTriggerTutorial() {
		System.out.println("First: you must have Cassandra with trigger patch.");
		System.out.println("You install as in https://issues.apache.org/jira/secure/attachment/12451779/HOWTO-PatchAndRunTriggerExample.txt");
		System.out.println("");
		System.out.println("After building assure that everything works than stop Cassandra and do the follow:");
		System.out.println("-Change static variables of desired trigger to match your configuration");
		System.out.println("-Add trigger to contrib/trigger_example/src");
		System.out.println("-Include trigger specifications in the end of cassandra.yaml at contrib/trigger_example");
		System.out.println("-Run \'ant\' to build");
		System.out.println("-Start Cassandra again");
		System.out.println("-It must be working now");
		System.out.println("");
		System.out.println("CDC-Triggered was made to handle triggered approaches");
	}
	
	public static void printUsage() {
		System.out.println("Usage:");
		System.out.println("  cdc [-verbose] [-prop] file [-option]");
		System.out.println("");
		System.out.println("  Be sure you use the correct order of parameters.");
		System.out.println("");
		System.out.println("Help Options:");
		System.out.println("  -h, -help                  Show help options");
		System.out.println("");
		System.out.println("Application Options:");
		System.out.println("  -triggers                  Show trigger short tutorial");
		System.out.println("  -v, -verbose               Verbose mode");
		System.out.println("  -p, -prop                  Set property file to be used");
		System.out.println("  -c, -createschema          Create Cassandra Schema");
		System.out.println("  -ca, -createaudit          Create Audit Columns to all columns of the Column Familys");
		System.out.println("  -pt, -preparetracking      Create and prepare Tracking Table based on atual data");
		System.out.println("  -ts, -tablescan            Table Scan CDC");
		System.out.println("  -ac, -auditcolumn          Audit Column CDC");
		System.out.println("  -lb, -logbased             Log based CDC");
		System.out.println("  -tt, -trackingtable        Tracking Table CDC");
		System.out.println("  -s, -snapshot              Snapshot creation");
		System.out.println("  -d, -differential          Differential CDC");		
		System.out.println("  -performance               To execute performance tests of any method");		
		System.out.println("");
		System.out.println("Remember to specify all fields in .properties file!!");
		System.out.println("");
	}
	
	public static void main (String[] args) throws Exception{
		
        int i = 0;//, j;
        String arg;
        //char flag;
        boolean vflag = false;
        String prop_file = "./CDC.properties";
        
        if (args.length < 1) {
        	printUsage();
        }
        
        while (i < args.length && args[i].startsWith("-")) {
            arg = args[i++];

            // use this type of check for "wordy" arguments
            if (arg.equals("-v") || arg.equals("-verbose")) {
                System.out.println(CDC +  "Verbose mode on");
                vflag = true;
            }
            
            else if (arg.equals("-timestamp")) {
                System.out.println(CDC +  "Current timestamp (ms): " + System.currentTimeMillis());
                vflag = true;
            }

            // use this type of check for arguments that require arguments
            else if (arg.equals("-p") || arg.equals("-prop")) {
                if (i < args.length)
                    prop_file = args[i++];
                else
                    System.err.println("-prop requires a property file");

            }
            
            else if (arg.equals("-performance")) {
        		PerformanceTest.main(prop_file, args[i]);            	
            }
            
            else if (arg.equals("-h") || arg.equals("-help")) {
            	printUsage();
            }            	
            
            else if (arg.equals("-ts") || arg.equals("-tablescan")) {
            	if (vflag) {
            		System.out.println(CDC + "Starting Table Scan...");
                    System.out.println(CDC + "Properties loaded from: " + prop_file);
            		TableScan.main(prop_file, "-verbose");
            	}
            	else
            		TableScan.main(prop_file);            	
            }
            
            else if (arg.equals("-s") || arg.equals("-snapshot")) {
            	if (vflag) {
            		System.out.println(CDC + "Starting Snapshot Storage...");
                    System.out.println(CDC + "Properties loaded from: " + prop_file);
            	}
            	Snapshot.main(prop_file);            	
            }
            
            else if (arg.equals("-d") || arg.equals("-differential")) {
            	if (vflag) {
            		System.out.println(CDC + "Starting Differential...");
                    System.out.println(CDC + "Properties loaded from: " + prop_file);
            		Differential.main(prop_file, "-verbose");
            	}
            	else
            		Differential.main(prop_file);            	
            }
            
            else if (arg.equals("-c") || arg.equals("-createschema")) {
            	if (vflag){
            		System.out.println(CDC + "Creating Schema...");
                    System.out.println(CDC + "Properties loaded from: " + prop_file);
            		Create.main(prop_file, "-verbose");
            	}
            	else
            		Create.main(prop_file);            	
            }
            
            else if (arg.equals("-lb") || arg.equals("-logbased")) {
            	if (vflag){            		
            		System.out.println(CDC + "Starting Log Based...");
                    System.out.println(CDC + "Properties loaded from: " + prop_file);
            		LogBased.main(prop_file, "-verbose");
            	}
            	else
            		LogBased.main(prop_file);            	
            }
            
            else if (arg.equals("-ac") || arg.equals("-auditcolumn")) {
            	if (vflag){
            		System.out.println(CDC + "Starting Audit Column CDC...");
                    System.out.println(CDC + "Properties loaded from: " + prop_file);
            		AuditColumn.main(prop_file, "-verbose");
            	}
            	else
            		AuditColumn.main(prop_file);            	
            }
            
            else if (arg.equals("-ca") || arg.equals("-createaudit")) {
            	if (vflag){            		
            		System.out.println(CDC + "Creating Audit Columns...");
                    System.out.println(CDC + "Properties loaded from: " + prop_file);
            		CreateAuditColumnsMapRed.main(prop_file, "-verbose");
            	}
            	else
            		CreateAuditColumnsMapRed.main(prop_file);            	
            }
            
            else if (arg.equals("-pt") || arg.equals("-preparetracking")) {
            	if (vflag){            		
            		System.out.println(CDC + "Creating and Preparing Tracking Table...");
                    System.out.println(CDC + "Properties loaded from: " + prop_file);
            		PrepareTrackingTableMapRed.main(prop_file, "-verbose");
            	}
            	else
            		PrepareTrackingTableMapRed.main(prop_file);            	
            }
            
            else if (arg.equals("-tt") || arg.equals("-trackingtable")) {
            	if (vflag){            		
            		System.out.println(CDC + "Starting Tracking Table CDC...");
                    System.out.println(CDC + "Properties loaded from: " + prop_file);
            		TrackingTableMapRed.main(prop_file, "-verbose");
            	}
            	else
            		TrackingTableMapRed.main(prop_file);            	
            }
            
            else if (arg.equals("-triggers")) {
            	printTriggerTutorial();         	
            }
            
            // use this type of check for a series of flag arguments
            else {
            	System.out.println(CDC + "Unknown option " + arg + ". Run \'cdc -help\' to see a full list of available command line options.");
               /* for (j = 1; j < arg.length(); j++) {
                    flag = arg.charAt(j);
                    switch (flag) {
                    case 'x':
                        if (vflag) System.out.println("Option x");
                        break;
                    case 'n':
                        if (vflag) System.out.println("Option n");
                        break;
                    default:
                        System.err.println("ParseCmdLine: illegal option " + flag);
                        break;
                    }
                }*/
            }
        }/*
        if (i == args.length)
        	System.out.println("Unknown options\nRun \'cdc -help\' to see a full list of available command line options.");
        else*/
        //System.out.println(CDC + "Done.");
        
    }		
}