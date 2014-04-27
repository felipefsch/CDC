package logBased;

import org.apache.hadoop.io.Text;

/**
 * This takes care of all operations with a LogRow object.
 * 
 * @author felipe
 *
 */
public class KeyValueGenerator {

	public static String generateKey(LogRow logRow) {
		String key = "";
		
		if (logRow.getKeyspace() == null) {
			return null;
		}
		else {
			key = logRow.getKeyspace() + "/"
					+ logRow.getColumnFamily() + "/"
					+ logRow.getKey();
		}
		
		return key;		
	}
	
	public static String generateValue(LogRow logRow) {
		String value = "";
		
		if (logRow.getKeyspace() == null) {
			return null;
		}
		else {
			// Detect deletion
			if (logRow.getOperation().equals(LogRow.MutationType.DELETION)) {
				value = "del/" +
							logRow.getSuperColumnName() + "/" +
							logRow.getColumnName() + "/" +
							logRow.getValue() + "/" +
							logRow.getTimestamp();
			}
			else if (logRow.getOperation().equals(LogRow.MutationType.INSERTION)) {
				value = "ins/" +
							logRow.getSuperColumnName() + "/" +
							logRow.getColumnName() + "/" +
							logRow.getValue() + "/" +
							logRow.getTimestamp();				
			}
		}		
		
		return value;
	}
	
	// Parse the Text input value of reducer to a Object for easier comparison at the logic
	public static LogRowValue createLRVObject(Text value) {
		LogRowValue valueObject = new LogRowValue();
		String inputValue = value.toString();
		
		String operation = inputValue.substring(0, inputValue.indexOf("/"));
		String unparsedInput = inputValue.substring(inputValue.indexOf("/") + 1, inputValue.length());
		String superColumn = unparsedInput.substring(0, unparsedInput.indexOf("/"));
		unparsedInput = unparsedInput.substring(unparsedInput.indexOf("/") + 1, unparsedInput.length());
		
		String column = unparsedInput.substring(0, unparsedInput.indexOf("/"));		
		unparsedInput = unparsedInput.substring(unparsedInput.indexOf("/") + 1, unparsedInput.length());
		
		String strValue = unparsedInput.substring(0, unparsedInput.indexOf("/"));
		unparsedInput = unparsedInput.substring(unparsedInput.indexOf("/") + 1, unparsedInput.length());
		
		String timestamp = unparsedInput.substring(0, unparsedInput.length());
		
		valueObject.setColumnName(column);
		valueObject.setSuperColumnName(superColumn);
		valueObject.setTimestamp(Long.parseLong(timestamp));
		valueObject.setValue(strValue);
		
		if (operation.equals("ins"))
			valueObject.setOperation(LogRow.MutationType.UPSERTION);
		else if (operation.equals("del"))
			valueObject.setOperation(LogRow.MutationType.DELETION);
		
		return valueObject; 
	}
	
}
