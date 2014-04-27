package logBased;


/**
 * This object keeps all data from the value part of a row in the CommitLogSegment.
 * 
 * @author felipe
 *
 */
public class LogRowValue {

	public String columnName;
	public String superColumnName;
	public long timestamp;
	public String value;
	
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getSuperColumnName() {
		return superColumnName;
	}
	public void setSuperColumnName(String superColumnName) {
		this.superColumnName = superColumnName;
	}	
	public LogRow.MutationType operation;
	
	public LogRow.MutationType getOperation() {
		return operation;
	}
	public void setOperation(LogRow.MutationType operation) {
		this.operation = operation;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public LogRowValue() {
		super();
		// TODO Auto-generated constructor stub
	}
	
    @Override
    public boolean equals(Object obj)
    {
        boolean isEqual = false;
        if (this.getClass() == obj.getClass())
        {
            LogRowValue logRowValue = (LogRowValue) obj;
            if ((logRowValue.getColumnName()).equals(this.getColumnName()) && 
            		(logRowValue.getOperation()).equals(this.getOperation()) && 
            		(logRowValue.getSuperColumnName()).equals(this.getSuperColumnName()) &&
            		(logRowValue.getTimestamp() == this.getTimestamp()) &&
            		(logRowValue.getValue()).equals(this.getValue()))
            {
                isEqual = true;
            }
        }
 
        return isEqual;
    }
	
}
