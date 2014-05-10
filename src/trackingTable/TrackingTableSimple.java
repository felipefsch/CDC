package trackingTable;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import utils.*;

public class TrackingTableSimple {
	private static String KEYSPACE = "School";
	private static String SOURCE_COLUMN_FAMILY = "Emulated";
	private static String TRACKING_COLUMN_FAMILY = "EmulatedTrackingTable";	
	private static String HOST = "localhost";
	private static int PORT = 9160;
	private static String UTF_8 = "UTF-8";
	
	private static String UP_OLD = "up_old";
	private static String UP_NEW = "up_new";
	private static String INS = "ins";
	private static String DEL = "del";
	private static long CURRENT_MAINTAINING_CYCLE = 0;
	private static long LAST_MAINTAINING_CYCLE = 0;
	
	private static String CYCLE_PATH = "/opt/apache-cassandra-trigger/cassandra/log/maintaining_cycle_v1.txt";
	private static String HDFS_OUTPUT = "hdfs://localhost:9000/triggerDifferentials/";
	private static String LOCAL_TMP_OUTPUT = "/opt/apache-cassandra-trigger/cassandra/trigger_files/";
	
	
	public static void main(String... args) throws Exception {
		
		refreshMaintainingTimes();
		
		HashMap<String, String> outputKeyValues = new HashMap<String, String>();
		Set<String> outputKeys = new HashSet<String>();
		String outputKey = "";
		String outputValue = "";
		
		List<KeySlice> keySlices = getKeySlice();
		
		for (KeySlice keySlice : keySlices) {
			String key = new String(keySlice.getKey(), UTF_8);
			byte[] byteKey = keySlice.getKey();
			
			List<ColumnOrSuperColumn> colOrSuperCols = keySlice.getColumns();
			
			for (ColumnOrSuperColumn colOrSuperCol : colOrSuperCols) {
				if (new String(colOrSuperCol.super_column.getName(), UTF_8).equals(UP_NEW)) {
					for (Column subColumn : colOrSuperCol.super_column.getColumns()) {
						
						if (subColumn.timestamp > LAST_MAINTAINING_CYCLE
								&& subColumn.timestamp < CURRENT_MAINTAINING_CYCLE)
						{					
							insertUpOld(byteKey, Utils.toString(subColumn.name),Utils.toString(subColumn.value));
							deleteSuperColumn(byteKey, UP_NEW, Utils.toString(subColumn.name));
							
							outputKey = outputUpdKey(key, new String(subColumn.getName(), UTF_8));
							outputValue = outputValue(subColumn.getValue());
							outputKeys.add(outputKey);
							outputKeyValues.put(outputKey, outputValue);
						}
					}
				}
				else if (new String(colOrSuperCol.super_column.getName(), UTF_8).equals(INS)) {
					for (Column subColumn : colOrSuperCol.super_column.getColumns()) {
						if (subColumn.timestamp > LAST_MAINTAINING_CYCLE
								&& subColumn.timestamp < CURRENT_MAINTAINING_CYCLE)
						{
							insertUpOld(byteKey, Utils.toString(subColumn.name), Utils.toString(subColumn.value));
							deleteSuperColumn(byteKey, INS, Utils.toString(subColumn.name));
							
							outputKey = outputInsKey(key, new String(subColumn.getName(), UTF_8));
							outputValue = outputValue(subColumn.getValue());
							outputKeys.add(outputKey);
							outputKeyValues.put(outputKey, outputValue);
						}
					}					
				}
				else if (new String(colOrSuperCol.super_column.getName(), UTF_8).equals(DEL)) {
					for (Column subColumn : colOrSuperCol.super_column.getColumns()) {
						if (subColumn.timestamp > LAST_MAINTAINING_CYCLE
								&& subColumn.timestamp < CURRENT_MAINTAINING_CYCLE)
						{
							deleteSuperColumn(byteKey, DEL, Utils.toString(subColumn.name));
							deleteSuperColumn(byteKey, UP_OLD, Utils.toString(subColumn.name));
							
							outputKey = outputDelKey(key, new String(subColumn.getName(), UTF_8));
							outputValue = outputValue(subColumn.getValue());
							outputKeys.add(outputKey);
							outputKeyValues.put(outputKey, outputValue);
						}
					}					
				}
			}			
		}
		
		String localSrc = LOCAL_TMP_OUTPUT + SOURCE_COLUMN_FAMILY + ".txt";// + CURRENT_MAINTAINING_CYCLE + ".txt";
		String dst = HDFS_OUTPUT + SOURCE_COLUMN_FAMILY; // + "-" + CURRENT_MAINTAINING_CYCLE;

		File f = new File(localSrc);
		if (!f.exists()) {
			f.createNewFile();
		}
		FileWriter fstream = new FileWriter(localSrc, true);
		BufferedWriter out = new BufferedWriter(fstream);
		
		// Here we iterate in the previews generated outputs for the current key and output them to HDFS
		// The output key-value is in the hash map previously filled.
		for (String key : outputKeys) {
			out.write(key + ":" + outputKeyValues.get(key) + "\n");		
		}
		
		out.close();
		
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream outs = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});
		
		IOUtils.copyBytes(in, outs, 4096, true);
	}
	
    private static void deleteSuperColumn(byte[] key, String superColumn, String column) throws Exception {
		// connecting to Cassandra
	    TTransport tr = new TFramedTransport(new TSocket(HOST, PORT));
	    TProtocol proto = new TBinaryProtocol(tr);
	    
	    // Cassandra thrift client
	    Cassandra.Client client = new Cassandra.Client(proto);
	    tr.open();	        	        
	    
	    client.set_keyspace(KEYSPACE);
    	
    	ColumnPath cp = new ColumnPath();
        cp.setColumn_family(TRACKING_COLUMN_FAMILY);
        cp.setColumn(column.getBytes(UTF_8));
        cp.setSuper_column(superColumn.getBytes(UTF_8));
        // Timestamp must be the one of the deletion time indeed?
        String strKey = new String(key, "UTF-8");
        client.remove(Utils.toByteBuffer(strKey), cp, (System.currentTimeMillis() * 1000), ConsistencyLevel.ALL);
        
        tr.close();
    }
	
	
	public static void outputResults(Set<String> outputKeys, HashMap<String, String> outputKeyValues) throws Exception {
		String localSrc = LOCAL_TMP_OUTPUT + SOURCE_COLUMN_FAMILY + ".txt";// + CURRENT_MAINTAINING_CYCLE + ".txt";
		String dst = HDFS_OUTPUT + SOURCE_COLUMN_FAMILY; // + "-" + CURRENT_MAINTAINING_CYCLE;
		
		File f = new File(localSrc);
		if (!f.exists()) {
			f.createNewFile();
		}
		FileWriter fstream = new FileWriter(localSrc, true);
		BufferedWriter out = new BufferedWriter(fstream);
		
		// Here we iterate in the previews generated outputs for the current key and output them to HDFS
		// The output key-value is in the hash map previously filled.
		for (String outputKey : outputKeys) {
			out.write(outputKey + ":" + outputKeyValues.get(outputKey) + "\n");		
		}
		
		out.close();
		
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream outs = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});
		
		IOUtils.copyBytes(in, outs, 4096, true);		
	}
	
	
	/**
	 * This creates a output key for update operations.
	 */
	private static String outputUpdKey(String key, String subColumnName) {
		return "upd/" + KEYSPACE + "/" + SOURCE_COLUMN_FAMILY + "/" + key + "/" + subColumnName;
	}
	
	/**
	 * This creates a output key for deletion operations.
	 */
	private static String outputDelKey(String key, String subColumnName) {
		return "del/" + KEYSPACE + "/" + SOURCE_COLUMN_FAMILY + "/" + key + "/" + subColumnName;
	}
	
	/**
	 * This creates a output key for insertion operations.
	 */
	private static String outputInsKey(String key, String subColumnName) {
		return "ins/" + KEYSPACE + "/" + SOURCE_COLUMN_FAMILY + "/" + key + "/" + subColumnName;	
	}
	
	/**
	 * This creates the output value.
	 * 
	 * @throws UnsupportedEncodingException 
	 */
	private static String outputValue(byte[] subColumnValue) throws UnsupportedEncodingException {
		String subColVal = new String(subColumnValue, UTF_8);
		return subColVal;
	}
	
	/**
	 * This initializates all the maintaining time constants.
	 * 
	 * @throws Exception 
	 */
	private static void refreshMaintainingTimes() throws Exception {
		// I do not guarantee the atomicity of the analysis and maintaining.
		File f = new File(CYCLE_PATH);
		if (!f.exists()) {
			f.createNewFile();
			FileWriter fstream = new FileWriter(CYCLE_PATH, false);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write("0");		
			out.close();
		}
		
		FileReader fsreader = new FileReader(f);
		BufferedReader in = new BufferedReader(fsreader);
		String lastMaintainingCycle = in.readLine();
		in.close();
		LAST_MAINTAINING_CYCLE = Long.parseLong(lastMaintainingCycle);
		
		CURRENT_MAINTAINING_CYCLE = System.currentTimeMillis() * 1000;
		FileWriter fstream = new FileWriter(CYCLE_PATH, false);
		BufferedWriter out = new BufferedWriter(fstream);
		out.write(Long.toString(CURRENT_MAINTAINING_CYCLE));		
		out.close();
	}
	
	private static void insertUpOld(byte[] key, String subColumnName, String value) throws Exception {
		// connecting to Cassandra
	    TTransport tr = new TFramedTransport(new TSocket(HOST, PORT));
	    TProtocol proto = new TBinaryProtocol(tr);
	    
	    // Cassandra thrift client
	    Cassandra.Client client = new Cassandra.Client(proto);
	    tr.open();	        	        
	    
	    client.set_keyspace(KEYSPACE);
	    
	    ColumnParent parent = new ColumnParent();
	    parent.setColumn_family(TRACKING_COLUMN_FAMILY);
	    parent.setSuper_column(UP_OLD.getBytes(UTF_8));
	    
	    Column column = new Column();
	    column.setName(subColumnName.getBytes(UTF_8));
	    column.setTimestamp(System.currentTimeMillis() * 1000);
	    //column.setClock(new Clock(System.currentTimeMillis() * 1000));
	    column.setValue(value.getBytes(UTF_8));
	    
	    String strKey = new String(key, "UTF-8");
	    client.insert(Utils.toByteBuffer(strKey), parent, column, ConsistencyLevel.ONE);
	}
	
	/**
	 * This connects to Cassandra and returns all the Super Columns and keys of a tracking table.
	 * 
	 * @return
	 * @throws InvalidRequestException
	 * @throws TException
	 * @throws UnavailableException
	 * @throws TimedOutException
	 */
	private static List<KeySlice> getKeySlice() throws InvalidRequestException, TException, UnavailableException, TimedOutException {
		// connecting to Cassandra
	    TTransport tr = new TFramedTransport(new TSocket(HOST, PORT));
	    TProtocol proto = new TBinaryProtocol(tr);
	    
	    // Cassandra thrift client
	    Cassandra.Client client = new Cassandra.Client(proto);
	    tr.open();	        	        
	    
	    client.set_keyspace(KEYSPACE);
	    
	    ColumnParent parent = new ColumnParent(TRACKING_COLUMN_FAMILY);
	    
	    SlicePredicate predicate = new SlicePredicate();
	    SliceRange sliceRange = new SliceRange();
	    sliceRange.setStart(new byte[0]);
	    sliceRange.setFinish(new byte[0]);	    
	    predicate.setSlice_range(sliceRange);
	    
	    KeyRange kr = new KeyRange();		
		kr.setEnd_key(new byte[0]);
		kr.setStart_key(new byte[0]);
		
		List<KeySlice> keySlices = client.get_range_slices(parent, predicate, kr, ConsistencyLevel.ALL);
	    
		tr.close();
		
	    return keySlices;
	}
}
