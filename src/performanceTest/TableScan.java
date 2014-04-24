package performanceTest;

public class TableScan {
	public static void main(String[] args) throws Exception {
		
		System.out.println("Running test of performance for Table Scan...");
		
		for (int i = 0; i < 10; i++) {
			long startTime = System.nanoTime();

			tableScan.TableScan.main();			
			
			long elapsedTime = System.nanoTime() - startTime;
				
			System.out.println("Elapsed time: " + elapsedTime + " for run number: " + i);
		}
	}
}
