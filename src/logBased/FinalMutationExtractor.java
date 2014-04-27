package logBased;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This have all necessary logic to extract the final mutations for the log based approach.
 * 
 * @author felipe
 *
 */
@SuppressWarnings("unused")
public class FinalMutationExtractor {

	private static LogRowValue searchKeyDeletion(List<LogRowValue> logRowValues) {
		LogRowValue deletedKey = null;
		
		// search in all mutations some key deletion
		for (LogRowValue lr : logRowValues) {
			if (lr.getColumnName().equals("null") && lr.getOperation().equals(LogRow.MutationType.DELETION)) {						
				if (deletedKey == null) {
					deletedKey = new LogRowValue();
					deletedKey = lr;
				}						
				// In case of more than one key deletion, consider just the last one
				else if (deletedKey.getTimestamp() < lr.getTimestamp()) {
					deletedKey = lr;
				}
			}
		}
		return deletedKey;
	}
	
	// Not used, but maybe it will be useful
	private static LogRowValue searchSuperColumnDeletion(List<LogRowValue> logRowValues) {
		LogRowValue deletedSuperColumn = null;
		
		// search in all mutations some key deletion
		for (LogRowValue lr : logRowValues) {
			if (lr.getColumnName().equals("null") && lr.getOperation().equals(LogRow.MutationType.DELETION) && !lr.getSuperColumnName().equals("null")) {						
				if (deletedSuperColumn == null) {
					deletedSuperColumn = new LogRowValue();
					deletedSuperColumn = lr;
				}						
				// In case of more than one key deletion, consider just the last one
				else if (deletedSuperColumn.getTimestamp() < lr.getTimestamp()) {
					deletedSuperColumn = lr;
				}
			}
		}		
		return deletedSuperColumn;
	}
	
	private static List<LogRowValue> getAfterDelMutations(LogRowValue deletedKey, List<LogRowValue> logRowValues) {
		List<LogRowValue> afterDelLogRows = new ArrayList<LogRowValue>();
		
		for (LogRowValue lr : logRowValues) {
			// consider just mutations after the key deletion
			if (lr.getTimestamp() > deletedKey.getTimestamp()) {
				afterDelLogRows.add(lr);
			}
		}		
		return afterDelLogRows;
	}
	
	/**
	 * This generates column deletions based on a key deletion.
	 * I assume there are mutations before the deletion, otherwise it will return null
	 * (thus makes you lost, at least, the key deletion operation).
	 * 
	 * @param deletedKey
	 * @param logRowValues
	 * @return
	 */
	private static List<LogRowValue> normalizeBeforeDelMutations(LogRowValue deletedKey, List<LogRowValue> logRowValues) {
		List<LogRowValue> beforeDelLogRows = new ArrayList<LogRowValue>();
		HashMap<String, String> insertedColumn = new HashMap<String, String>();
		
		for (LogRowValue lr : logRowValues) {
			// Transform key deletion into column deletions for all operations
			if (lr.getTimestamp() < deletedKey.getTimestamp()
					&& insertedColumn.get(lr.getSuperColumnName() + lr.getColumnName()) == null
					&& lr.getOperation().equals(LogRow.MutationType.INSERTION)) {
				
				insertedColumn.put(lr.getSuperColumnName() + lr.getColumnName(), lr.getValue());
				lr.setOperation(LogRow.MutationType.DELETION);
				// should it be a tombstone like in cassandra? How create it?
				lr.setValue("tombstone");
				// Use the TS of the key deletion as the TS here
				lr.setTimestamp(deletedKey.getTimestamp());
				beforeDelLogRows.add(lr);
			}
		}		
		return beforeDelLogRows;
	}
	
	/**
	 * This generates Standard Column deletion based on a super column deletion.
	 * I assume you have previews mutations, otherwise it returns null
	 * (Thus makes you lost, at least, the super column deletion operation)
	 * 
	 * @param deletedSuperColumn
	 * @param logRowValues
	 * @return
	 */
	// its not used, but it may be useful
	private static List<LogRowValue> getBeforeSuperColumnDeletionMutations(LogRowValue deletedSuperColumn, List<LogRowValue> logRowValues) {
		List<LogRowValue> beforeDelLogRows = new ArrayList<LogRowValue>();
		HashMap<String, String> insertedColumn = new HashMap<String, String>();
		
		for (LogRowValue lr : logRowValues) {
			// Consider just mutations for the same super column
			if (lr.getSuperColumnName().equals(deletedSuperColumn.getSuperColumnName())) {
				// Transform super column deletion into sub-column deletions
				if (lr.getTimestamp() < deletedSuperColumn.getTimestamp()
						&& insertedColumn.get(lr.getSuperColumnName() + lr.getColumnName()) == null
						&& lr.getOperation().equals(LogRow.MutationType.INSERTION)) {
					
					insertedColumn.put(lr.getSuperColumnName() + lr.getColumnName(), lr.getValue());
					lr.setOperation(LogRow.MutationType.DELETION);
					// should it be a tombstone like in cassandra? How create it?
					lr.setValue("tombstone");
					// Use the TS of the key deletion as the TS here
					lr.setTimestamp(deletedSuperColumn.getTimestamp());
					beforeDelLogRows.add(lr);
				}
			}
			else {
				// add all mutations for the other super columns
				if(lr.getTimestamp() < deletedSuperColumn.getTimestamp()) {
					beforeDelLogRows.add(lr);
				}
			}
		}
		return beforeDelLogRows;
	}
	
	private static LogRowValue getNewestLogRowValue (LogRowValue lr1, LogRowValue lr2) {		
		if (lr1.getTimestamp() > lr2.getTimestamp())
			return lr1;
		else
			return lr2;
	}
	/**
	 * This computes the resulting mutation giving the last two mutations for a superColumn/column.
	 * 
	 * @param newLRV
	 * @param oldLRV
	 * @return
	 */
	private static LogRowValue getFinalLogRowValue (LogRowValue newLRV, LogRowValue oldLRV) {
		LogRowValue finalLRV = new LogRowValue();
		
		if (oldLRV.getTimestamp() > 0) {
			// Special case of an update
			if (newLRV.getOperation().equals(LogRow.MutationType.INSERTION) && oldLRV.getOperation().equals(LogRow.MutationType.INSERTION)) {
				newLRV.setOperation(LogRow.MutationType.UPDATE);
				finalLRV = newLRV;
			}
			else {
				finalLRV = newLRV;
			}			
		}
		else {
			finalLRV = newLRV;
		}
		
		return finalLRV;
	}
	
	/**
	 * This analysis all the mutations to find the two newest ones, using it to define the resulting mutation.
	 * 
	 * @param logRowValues
	 * @return
	 */
	private static List<LogRowValue> computesFinalMutations(List<LogRowValue> logRowValues) {
		HashMap<String, LogRowValue> analysedCols = new HashMap<String, LogRowValue>();
		List<LogRowValue> finalLogRows = new ArrayList<LogRowValue>();
		
		for (LogRowValue lr : logRowValues) {
			// index of current object
			int index = logRowValues.indexOf(lr);			
			LogRowValue lastMutation = new LogRowValue();
			LogRowValue secondMutation = new LogRowValue();
			LogRowValue firstMutation = new LogRowValue();

			// We assume it for the case of just one mutation
			firstMutation = lr;
			
			// iterate in the rest part of the list to find the last 2 mutations for the same column/superColumn
			for (int i = (index + 1); i < logRowValues.size(); i++) {
				// Mutation for the same row
				if (logRowValues.get(i).getColumnName().equals(lr.getColumnName()) && logRowValues.get(i).getSuperColumnName().equals(lr.getSuperColumnName())) {
					LogRowValue auxLRV = getNewestLogRowValue(lr, logRowValues.get(i));
					
					if (auxLRV.getTimestamp() >= firstMutation.getTimestamp()) {
						secondMutation = firstMutation;
						firstMutation = auxLRV;
					}
					else if (auxLRV.getTimestamp() > secondMutation.getTimestamp() && auxLRV.getTimestamp() < secondMutation.getTimestamp()) {
						secondMutation = auxLRV;
					}						
				}
			}
			// Add newest mutation for the actual logRow.
			// Hash table is used to do not consider later the keys already analyzed
			if (analysedCols.get(lr.getSuperColumnName() + lr.getColumnName()) == null) {
				lastMutation = getFinalLogRowValue(firstMutation, secondMutation);
				finalLogRows.add(lastMutation);
				analysedCols.put(lr.getSuperColumnName() + lr.getColumnName(), lastMutation);
			}							
		}
		return finalLogRows;
	}
	
	/**
	 * This gives the interface for outer classes.
	 * Giving a list of logRowValues for one keyspace/columnFamily/key, computes the resulting mutation
	 * considering all mutations realized for each superColumn/column
	 * 
	 * @param logRowValues
	 * @return
	 */
	public static List<LogRowValue> extractMutations(List<LogRowValue> logRowValues) {
		List<LogRowValue> resultingLRV = new ArrayList<LogRowValue>();
		
		// Search for deletions
		LogRowValue deletedKey = searchKeyDeletion(logRowValues);
//		LogRowValue deletedSuperColumn = searchSuperColumnDeletion(logRowValues);
		
		
		if (deletedKey != null) {
			// finalLogRows.add(deletedKey);
			
			// it is necessary because this two sets of mutation must be treated differently
			List<LogRowValue> afterDelLRV = getAfterDelMutations(deletedKey, logRowValues);
			List<LogRowValue> beforeDelLRV = normalizeBeforeDelMutations(deletedKey, logRowValues);
			
			// This list will contain mutations in a 'final pattern'
			afterDelLRV.addAll(beforeDelLRV);
			
			// Just one mutation, no need of further analysis
			if (afterDelLRV.size() == 1) {
				resultingLRV.add(afterDelLRV.get(0));
			}
			// Analyze just relevant rows
			else if (afterDelLRV.size() > 1){						
				resultingLRV.addAll(computesFinalMutations(afterDelLRV));
			}
		}
		else {
			// Analyze all rows
			resultingLRV.addAll(computesFinalMutations(logRowValues));
		}		
		return resultingLRV;
	}
}
