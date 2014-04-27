import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.trigger.ITrigger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


public class LoggingTrigger implements ITrigger {
	
	// Variables to be setted following your configuration/desire
	private static String LOG_FILE = "/opt/log.txt";
	
	private static String KEYSPACE = "TriggerExample";
	private static String COLUMN_FAMILY = "Data";
	
	private static String ADDRESS = "localhost";
	private static int RPC_PORT = 9160;
	// End of variables to be configured

	private static String UTF_8 = "UTF-8";	
	
	private static final int localDeletionTime = -2147483648;
	private static final long marketForDeleteAt = Long.parseLong("-9223372036854775808");

	@Override
	public void execute(byte[] key, ColumnFamily cf) {		
		try {
			FileWriter fstream = new FileWriter(LOG_FILE, true);
			BufferedWriter out = new BufferedWriter(fstream);
			//out.append("Will log!");
			//out.newLine();
			
			// connecting to Cassandra
	        TTransport tr = new TFramedTransport(new TSocket(ADDRESS, RPC_PORT));
	        TProtocol proto = new TBinaryProtocol(tr);
	        
	        // Cassandra thrift client
	        Cassandra.Client client = new Cassandra.Client(proto);
	        tr.open();
	        client.set_keyspace(KEYSPACE);       
			
			String logRow = null;
			int localDeletionTime = cf.getLocalDeletionTime();
			
			if (cf.isSuper()) {
				//out.append("SuperColumn");
				//out.newLine();
				serializeSuperColumn(key, cf);
			}
			else {
				//out.append("StandardColumn");
				//out.newLine();
				serializeStandardColumn(key, cf);
			}
			out.close();
		}
		catch (Exception e) {
			
		}
	}
	
	private static void serializeStandardColumn(byte[] key, ColumnFamily cf) throws IOException {
		// must have the following structure
		// ks/key/1/colFam/true/colfam/localDelTime/marketfordeleteat/1-0/
		try {
			FileWriter fstream = new FileWriter(LOG_FILE, true);
			BufferedWriter out = new BufferedWriter(fstream);
			
			Collection<IColumn> columns = cf.getSortedColumns();
			
			if (cf.isMarkedForDelete()) {
				out.append("[" + KEYSPACE + "][" + new String(key, UTF_8) + "][1][" + COLUMN_FAMILY + "][true][" +
						cf.getLocalDeletionTime() + "][" + (System.currentTimeMillis() * 1000) + "][0]");
				out.newLine();				
			}
			else {			
				for (IColumn column : columns) {
					
					if (column.getClass().getSimpleName().equals("Column")) {
						out.append("[" + KEYSPACE + "][" + new String(key, UTF_8) + "][1][" + COLUMN_FAMILY + "][true][" +
								localDeletionTime + "][" + marketForDeleteAt + "][1][" + new String(column.name(), UTF_8) + "][0][" +
								(System.currentTimeMillis() * 1000) + "][" + new String(column.value(), UTF_8) + "]");
						out.newLine();
					}
					else if (column.getClass().getSimpleName().equals("DeletedColumn")) {
						out.append("[" + KEYSPACE + "][" + new String(key, UTF_8) + "][1][" + COLUMN_FAMILY + "][true][" +
								localDeletionTime + "][" + marketForDeleteAt + "][1][" + new String(column.name(), UTF_8) + "][1][" +
								(System.currentTimeMillis() * 1000) + "][" + new String(column.value(), UTF_8) + "]");
						out.newLine();
					}
				}
			}
			out.close();
		}
		catch (Exception e) {
			FileWriter fstream = new FileWriter(LOG_FILE, true);
			BufferedWriter out = new BufferedWriter(fstream);
			
			//out.append("deu Zebra: " + e);
			//out.newLine();
			out.close();
		}
	}

	private static void serializeSuperColumn(byte[] key, ColumnFamily cf) throws IOException {
		FileWriter fstream = new FileWriter(LOG_FILE, true);
		BufferedWriter out = new BufferedWriter(fstream);
		
		if (cf.isMarkedForDelete()) {
			out.append("[" + KEYSPACE + "][" + new String(key, UTF_8) + "][1][" + COLUMN_FAMILY + "][true][" +
					cf.getLocalDeletionTime() + "][" + (System.currentTimeMillis() * 1000) + "][0]");
			out.newLine();
		}
		else {
			Collection<IColumn> superColumns = cf.getSortedColumns();
			for (IColumn superColumn : superColumns) {
				if (superColumn.isMarkedForDelete()) {
					out.append("[" + KEYSPACE + "][" + new String(key, UTF_8) + "][1][" + COLUMN_FAMILY + "][true][" +
							localDeletionTime + "][" + marketForDeleteAt + "][1][" + new String(superColumn.name(), UTF_8) + "][0][" +
							superColumn.getLocalDeletionTime() + "][" + (System.currentTimeMillis() * 1000) + "]");
					out.newLine();					
				}
				else {
					Collection<IColumn> subColumns = superColumn.getSubColumns();
//					if (superColumn.getClass().getSimpleName().equals("SuperColumn")) {
//						SuperColumn col = (SuperColumn) superColumn;
//						out.append("[" + KEYSPACE + "][" + new String(key, UTF_8) + "][1][" + COLUMN_FAMILY + "][true][" +
//								localDeletionTime + "][" + marketForDeleteAt + "][1][" + new String(col.name(), UTF_8) + "][0][" +
//								(System.currentTimeMillis() * 1000) + "][" + new String(col.value(), UTF_8) + "]");
//						out.newLine();
//					}			
					for (IColumn subColumn : subColumns) {
						if (subColumn.getClass().getSimpleName().equals("Column")) {					
							out.append("[" + KEYSPACE + "][" + new String(key, UTF_8) + "][1][" + COLUMN_FAMILY + "][true][" +
									localDeletionTime + "][" + marketForDeleteAt + "][1][" + new String(superColumn.name(), UTF_8) + "][1][" +
									localDeletionTime + "][" + marketForDeleteAt + "][1][" + new String(subColumn.name(), UTF_8) + "][0][" +
									(System.currentTimeMillis() * 1000) + "][" + new String(subColumn.value(), UTF_8) + "]");
							out.newLine();
						}
						else if (subColumn.getClass().getSimpleName().equals("DeletedColumn")) {
							out.append("[" + KEYSPACE + "][" + new String(key, UTF_8) + "][1][" + COLUMN_FAMILY + "][true][" +
									localDeletionTime + "][" + marketForDeleteAt + "][1][" + new String(superColumn.name(), UTF_8) + "][1][" +
									localDeletionTime + "][" + marketForDeleteAt + "][1][" + new String(subColumn.name(), UTF_8) + "][1][" +
									(System.currentTimeMillis() * 1000) + "][" + new String(subColumn.value(), UTF_8) + "]");
							out.newLine();
						}
					}
				}
			}
		}
		out.close();
	}
	/*
    public static ByteBuffer toByteBuffer(String value) 
    throws UnsupportedEncodingException
    {
        return ByteBuffer.wrap(value.getBytes("UTF-8"));
    }
        
    public static String toString(ByteBuffer buffer) 
    throws UnsupportedEncodingException
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, "UTF-8");
    }*/
}
