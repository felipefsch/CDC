package performanceTest;

import logBased.LogBased;
import schema.Create;
import snapshotDifferential.Differential;
import snapshotDifferential.Snapshot;
import tableScan.TableScan;
import trackingTable.PrepareTrackingTable;
import trackingTable.TrackingTableSimple;
import auditColumn.AuditColumn;
import auditColumn.CreateAuditColumns;

public class PerformanceTest {
	public static void main(String... args) throws Exception {
		
		String prop_file = args[0];
		String arg = args.length > 1 ? args[1] : null;	
		
		int numRuns = 30;
		
		if (arg.equals("-ts") || arg.equals("-tablescan")) {
			System.out.println("Running test of performance for Table Scan...");
			
			for (int i = 0; i < numRuns; i++) {
				long startTime = System.nanoTime();

				TableScan.main(prop_file); 			
				
				long elapsedTime = System.nanoTime() - startTime;
					
				System.out.println("Run: " + i + " time: " + elapsedTime);
			}        		           	
        }
        
        else if (arg.equals("-s") || arg.equals("-snapshot")) {
			System.out.println("Running test of performance for Snapshot...");
			
			for (int i = 0; i < numRuns; i++) {
				long startTime = System.nanoTime();

				Snapshot.main(prop_file);  			
				
				long elapsedTime = System.nanoTime() - startTime;
					
				System.out.println("Run: " + i + " time: " + elapsedTime);
			}       	          
        }
        
        else if (arg.equals("-d") || arg.equals("-differential")) {
			System.out.println("Running test of performance for Differential...");
			
			for (int i = 0; i < numRuns; i++) {
				long startTime = System.nanoTime();

				Differential.main(prop_file);  			
				
				long elapsedTime = System.nanoTime() - startTime;
					
				System.out.println("Run: " + i + " time: " + elapsedTime);
			}        		            	
        }
        
        else if (arg.equals("-lb") || arg.equals("-logbased")) {
			System.out.println("Running test of performance for LogBased...");
			
			for (int i = 0; i < numRuns; i++) {
				long startTime = System.nanoTime();

				LogBased.main(prop_file);  			
				
				long elapsedTime = System.nanoTime() - startTime;
					
				System.out.println("Run: " + i + " time: " + elapsedTime);
			}        		            	
        }
        
        else if (arg.equals("-ac") || arg.equals("-auditcolumn")) {
			System.out.println("Running test of performance for AuditColumn...");
			
			for (int i = 0; i < numRuns; i++) {
				long startTime = System.nanoTime();

				AuditColumn.main(prop_file);  			
				
				long elapsedTime = System.nanoTime() - startTime;
					
				System.out.println("Run: " + i + " time: " + elapsedTime);
			}        	            	
        }
        
        else if (arg.equals("-tt") || arg.equals("-trackingtable")) {
			System.out.println("Running test of performance for TrackingTable...");
			
			for (int i = 0; i < numRuns; i++) {
				long startTime = System.nanoTime();

				TrackingTableSimple.main(prop_file);   			
				
				long elapsedTime = System.nanoTime() - startTime;
					
				System.out.println("Run: " + i + " time: " + elapsedTime);
			}
        }	
	}
}
