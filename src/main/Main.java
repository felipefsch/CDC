package main;

import tableScan.TableScan;
import snapshotDifferential.Snapshot;
import snapshotDifferential.Differential;
import schema.*;

public class Main {

	public static void printUsage() {
		System.out.println("Usage:");
		System.out.println("  cdc [OPTION...] [FILE...]");
		System.out.println("");
		System.out.println("Help Options:");
		System.out.println("  -help                      Show help options");
		System.out.println("");
		System.out.println("Application Options:");
		System.out.println("  -prop                      Set property file with all properties");
		System.out.println("  -tablescan                 Execute tablescan CDC");
		System.out.println("  -logbased                  Execute logbased CDC");
		System.out.println("  -snapshot                  Create database snapshot");
		System.out.println("  -differential              Execute differential");
		System.out.println("  -createschema              Create Cassandra Schema");
		System.out.println("");
		System.out.println("Remember specify all fields in .properties file!!");
	}
	
	public static void main (String[] args) throws Exception{
		
        int i = 0;//, j;
        String arg;
        //char flag;
        boolean vflag = false;
        String prop_file = "./CDC.properties";

        while (i < args.length && args[i].startsWith("-")) {
            arg = args[i++];

            // use this type of check for "wordy" arguments
            if (arg.equals("-verbose")) {
                System.out.println("verbose mode on");
                vflag = true;
            }

            // use this type of check for arguments that require arguments
            else if (arg.equals("-prop")) {
                if (i < args.length)
                    prop_file = args[i++];
                else
                    System.err.println("-prop requires a property file");
                if (vflag)
                    System.out.println("Properties file = " + prop_file);
            }
            
            else if (arg.equals("-help")) {
            	printUsage();
            }            	
            
            else if (arg.equals("-tablescan")) {
            	if (vflag) System.out.println("Starting Table Scan...");
            	TableScan.main(prop_file);            	
            }
            
            else if (arg.equals("-snapshot")) {
            	if (vflag) System.out.println("Starting Snapshot Storage...");
            	Snapshot.main(prop_file);            	
            }
            
            else if (arg.equals("-differential")) {
            	if (vflag) System.out.println("Starting Differential...");
            	Differential.main(prop_file);            	
            }
            
            else if (arg.equals("-createschema")) {
            	if (vflag) System.out.println("Creating Schema...");
            	CreateSchema.main(prop_file);            	
            }

            // use this type of check for a series of flag arguments
            else {
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
        }
        if (i == args.length)
        	System.out.println("Unknown options\nRun \'cdc -help\' to see a full list of available command line options.");
        else
            System.out.println("Done.");
    }		
}