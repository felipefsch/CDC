package main;

import tableScan.TableScan;
import snapshotDifferential.Snapshot;
import snapshotDifferential.Differential;
import schema.*;

public class Main {
	
	private static String CDC = utils.Utils.CDC;
	
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
		System.out.println("  -v, -verbose               Verbose mode");
		System.out.println("  -p, -prop                  Set property file with all properties");
		System.out.println("  -ts, -tablescan            Table Scan CDC");
		System.out.println("  -lb, logbased              Execute logbased CDC");
		System.out.println("  -s, -snapshot              Snapshot creation");
		System.out.println("  -d, -differential          Differential CDC");
		System.out.println("  -c, -createschema          Create Cassandra Schema");
		System.out.println("");
		System.out.println("Remember to specify all fields in .properties file!!");
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

            // use this type of check for arguments that require arguments
            else if (arg.equals("-p") || arg.equals("-prop")) {
                if (i < args.length)
                    prop_file = args[i++];
                else
                    System.err.println("-p requires a property file");
                if (vflag)
                    System.out.println(CDC + "Properties loaded from: " + prop_file);
            }
            
            else if (arg.equals("-h") || arg.equals("-help")) {
            	printUsage();
            }            	
            
            else if (arg.equals("-ts") || arg.equals("-tablescan")) {
            	if (vflag) {
            		System.out.println(CDC + "Starting Table Scan...");
            		TableScan.main(prop_file, "-verbose");
            	}
            	else
            		TableScan.main(prop_file);            	
            }
            
            else if (arg.equals("-s") || arg.equals("-snapshot")) {
            	if (vflag) System.out.println(CDC + "Starting Snapshot Storage...");
            	Snapshot.main(prop_file);            	
            }
            
            else if (arg.equals("-d") || arg.equals("-differential")) {
            	if (vflag) {
            		System.out.println(CDC + "Starting Differential...");
            		Differential.main(prop_file, "-verbose");
            	}
            	else
            		Differential.main(prop_file);            	
            }
            
            else if (arg.equals("-c") || arg.equals("-createschema")) {
            	if (vflag){
            		System.out.println(CDC + "Creating Schema...");
            		Create.main(prop_file, "-verbose");
            	}
            	else
            		Create.main(prop_file);            	
            }

            else if (arg.equals("-lb") || arg.equals("-logbased")) {
            	if (vflag){            		
            		System.out.println(CDC + "Starting Log Based...");
            		//call logbased
            	}
            	else
            		printUsage();//Create.main(prop_file);            	
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
        System.out.println(CDC + "Done.");
        
    }		
}