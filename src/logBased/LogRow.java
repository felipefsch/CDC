package logBased;

/**
 * This keeps all necessary data for a LogRow into the CommitLogSegments.
 * @author felipe
 *
 */
public class LogRow {

	public enum MutationType {
		COUNTER {
				public String toString() {
					return "COUNTER";
				}
		},
		COUNTER_UPDATE {
				public String toString() {
					return "COUNTER_UPDATE";
				}
		},
		DELETION  {
				public String toString() {
					return "del";
				}
		},
		EXPIRATION {
				public String toString() {
					return "EXPIRATION";
				}
		},
		INSERTION {
				public String toString() {
					return "ins";
			}
		},
		UPSERTION {
			public String toString() {
				return "upsert";
			}
		},
		UPDATE {
				public String toString() {
					return "upd";
				}
		};
	}
	
	// Key
	public String keyspace;
	public String columnFamily;
	public String key;
	public String columnName;
	public String superColumnName;
	public Integer columnFamilyId;

	// Value
	public long timestamp;
	public String value;
	
	// Operation
	public MutationType mutationType;
	
	// More infos
	public int numberModifications;
	public boolean bool;
	public int localDeletionTime;
	public long marketForDeleteAt;
	public int numberStandardColumns;
	public int numberSuperColumns;
	public int superColumnLocalDeletionTime;
	public long superColumnMarketForDeleteAt;

	
	
	public Integer getColumnFamilyId() {
		return columnFamilyId;
	}

	public void setColumnFamilyId(Integer columnFamilyId) {
		this.columnFamilyId = columnFamilyId;
	}

	public MutationType getMutationType() {
		return mutationType;
	}

	public void setMutationType(MutationType mutationType) {
		this.mutationType = mutationType;
	}

	public int getNumberModifications() {
		return numberModifications;
	}

	public void setNumberModifications(int numberModifications) {
		this.numberModifications = numberModifications;
	}

	public boolean isBool() {
		return bool;
	}

	public void setBool(boolean bool) {
		this.bool = bool;
	}

	public int getLocalDeletionTime() {
		return localDeletionTime;
	}

	public void setLocalDeletionTime(int localDeletionTime) {
		this.localDeletionTime = localDeletionTime;
	}

	public long getMarketForDeleteAt() {
		return marketForDeleteAt;
	}

	public void setMarketForDeleteAt(long marketForDeleteAt) {
		this.marketForDeleteAt = marketForDeleteAt;
	}

	public int getNumberStandardColumns() {
		return numberStandardColumns;
	}

	public void setNumberStandardColumns(int numberStandardColumns) {
		this.numberStandardColumns = numberStandardColumns;
	}

	public int getNumberSuperColumns() {
		return numberSuperColumns;
	}

	public void setNumberSuperColumns(int numberSuperColumns) {
		this.numberSuperColumns = numberSuperColumns;
	}

	public int getSuperColumnLocalDeletionTime() {
		return superColumnLocalDeletionTime;
	}

	public void setSuperColumnLocalDeletionTime(int superColumnLocalDeletionTime) {
		this.superColumnLocalDeletionTime = superColumnLocalDeletionTime;
	}

	public long getSuperColumnMarketForDeleteAt() {
		return superColumnMarketForDeleteAt;
	}

	public void setSuperColumnMarketForDeleteAt(long superColumnMarketForDeleteAt) {
		this.superColumnMarketForDeleteAt = superColumnMarketForDeleteAt;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

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

	public MutationType getOperation() {
		return mutationType;
	}

	public void setOperation(logBased.LogRow.MutationType operation) {
		this.mutationType = operation;
	}

	public LogRow() {
		super();
		// TODO Auto-generated constructor stub
	}

	
}
