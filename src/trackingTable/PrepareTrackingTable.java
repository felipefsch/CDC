package trackingTable;
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Clock;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import utils.Utils;

public class PrepareTrackingTable {

	private static String KEYSPACE = "School";
	private static String SOURCE_TABLE = "Grades";
	private static String TRACKING_TABLE = "GradesTracking";
	private static boolean IS_SUPER = false;
	
	private static String RPC_PORT = "9160";
	private static String ADDRESS = "localhost";
	
	private static Properties prop;
    
    private static String PROP_PATH = "./CDC.properties";
    
    private static boolean VERBOSE = false;
    
    private static String CDC = Utils.CDC;
	
	public static void main (String... args) throws Exception {
		
		PROP_PATH = args.length > 0 ? args[0] : "./CDC.properties";    	
    	VERBOSE = args.length > 1 && args[1].equals("-verbose") ? true : false;    	
    	prop = Utils.getCassandraProp(PROP_PATH);    	
    	KEYSPACE = prop.getProperty("cassandra.keyspace");    	
    	SOURCE_TABLE = prop.getProperty("cassandra.column_family");
    	TRACKING_TABLE = prop.getProperty("tracking_table.column_family");
    	IS_SUPER = prop.getProperty("cassandra.is_super_cf").equals("true") ? true : false;
    	
    	RPC_PORT = prop.getProperty("cassandra.rpc_port");
    	ADDRESS = prop.getProperty("cassandra.address");
		
		startTrackingTable();		
		
		if (IS_SUPER) {
			if (VERBOSE)
				System.out.println(CDC + "Will prepare tracking table for a Super Column Family");
				
			prepareSuperColumnTracking();
		}
		else {
			if (VERBOSE)
				System.out.println(CDC + "Will prepare tracking table for a Standard Column Family");
			
			prepareStandardColumnTracking();
		}
	}
	
	private static void startTrackingTable() throws InvalidRequestException, TException {
		// connecting to Cassandra
        TTransport tr = new TFramedTransport(new TSocket("localhost", 9160));
        TProtocol proto = new TBinaryProtocol(tr);
        
        // Cassandra thrift client
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();
        
        client.set_keyspace(KEYSPACE);
		try {	        
	        try {
	        	client.system_drop_column_family(TRACKING_TABLE);
	        }
	        catch (Exception e1) {
	        	System.out.println(CDC + Utils.ERROR + "Could not drop " + TRACKING_TABLE + ". Will create new table!");
	        }
	        CfDef cf = new CfDef();
	        cf.setColumn_type("Super");
	        cf.setKeyspace(KEYSPACE);
	        cf.setName(TRACKING_TABLE);
	        cf.setComparator_type("UTF8Type");
	        cf.setSubcomparator_type("UTF8Type");
	        client.system_add_column_family(cf);
	        
	        if (VERBOSE)
	        	System.out.println(CDC + "Fresh Tracking Table " + TRACKING_TABLE + " created");
		}
		catch (Exception e) {
			if (VERBOSE)
				System.out.println(CDC + Utils.ERROR + "Tracking Table Creation Error");
		}
		finally {
			
			tr.close();
		}
	}
	
	private static void prepareStandardColumnTracking() throws Exception {
		// connecting to Cassandra
        TTransport tr = new TFramedTransport(new TSocket(ADDRESS, Integer.parseInt(RPC_PORT)));
        TProtocol proto = new TBinaryProtocol(tr);
        
        // Cassandra thrift client
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();	        	        
        
        client.set_keyspace(KEYSPACE);
        
		KeyRange kr = new KeyRange();		
		kr.setEnd_key(new byte[0]);
		kr.setStart_key(new byte[0]);
		
		ColumnParent cp = new ColumnParent(SOURCE_TABLE);
    	
        SlicePredicate sp = new SlicePredicate();	        
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]);
        sliceRange.setFinish(new byte[0]);
        sp.setSlice_range(sliceRange);
		
		List<KeySlice> keySlices = client.get_range_slices(cp, sp, kr, ConsistencyLevel.ONE);
		
		for (KeySlice keySlice : keySlices) {
			byte[] key = keySlice.getKey();
			String strKey = new String(key, "UTF-8");
			List<ColumnOrSuperColumn> colOrSuperCols = keySlice.getColumns();
			
			for (ColumnOrSuperColumn colOrSuperCol : colOrSuperCols) {
				String finalValue = Utils.toString(colOrSuperCol.column.value);
				ColumnParent parent = new ColumnParent();
				parent.setColumn_family(TRACKING_TABLE);
				parent.setSuper_column("up_old".getBytes("UTF-8"));
		        Column c = new Column();//colOrSuperCol.column.name, finalValue.getBytes("UTF-8"), new Clock(colOrSuperCol.column.clock.timestamp));
		        
		        c.setName(colOrSuperCol.column.name);
		        c.setValue(finalValue.getBytes("UTF-8"));
		        c.setTimestamp(colOrSuperCol.column.timestamp);
		        
		        client.insert(Utils.toByteBuffer(strKey), parent, c, ConsistencyLevel.ONE);
				
//				System.out.println(new String(key, "UTF-8"));
//				System.out.println(new String(colOrSuperCol.column.name, "UTF-8"));
//				System.out.println(new String(colOrSuperCol.column.value, "UTF-8"));
//				System.out.println(colOrSuperCol.column.clock.timestamp);
			}
		}
		
		if (VERBOSE)
			System.out.println(CDC + "Tracking table " + TRACKING_TABLE + " prepared!");
        
	}
	
	private static void prepareSuperColumnTracking() throws Exception {
		// connecting to Cassandra
        TTransport tr = new TFramedTransport(new TSocket(ADDRESS, Integer.parseInt(RPC_PORT)));
        TProtocol proto = new TBinaryProtocol(tr);
        
        // Cassandra thrift client
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();	        	        
        
        client.set_keyspace(KEYSPACE);
        
		KeyRange kr = new KeyRange();		
		kr.setEnd_key(new byte[0]);
		kr.setStart_key(new byte[0]);
		
		ColumnParent cp = new ColumnParent(SOURCE_TABLE);
    	
        SlicePredicate sp = new SlicePredicate();	        
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]);
        sliceRange.setFinish(new byte[0]);
        sp.setSlice_range(sliceRange);
		
		List<KeySlice> keySlices = client.get_range_slices(cp, sp, kr, ConsistencyLevel.ONE);
		
		for (KeySlice keySlice : keySlices) {
			byte[] key = keySlice.getKey();
			String strKey = new String(key, "UTF-8");
			List<ColumnOrSuperColumn> colOrSuperCols = keySlice.getColumns();
			
			for (ColumnOrSuperColumn colOrSuperCol : colOrSuperCols) {				
				for (Column column : colOrSuperCol.super_column.columns) {
					String finalValue = Utils.toString(column.value);
					ColumnParent parent = new ColumnParent();
					parent.setColumn_family(TRACKING_TABLE);
					parent.setSuper_column("up_old".getBytes("UTF-8"));
					String finalColName = Utils.toString(colOrSuperCol.super_column.name) + "/" + Utils.toString(column.name);  
			        Column c = new Column();
			        
			        c.setName(colOrSuperCol.column.name);
			        c.setValue(finalValue.getBytes("UTF-8"));
			        c.setTimestamp(colOrSuperCol.column.timestamp);
			        
			        client.insert(Utils.toByteBuffer(strKey), parent, c, ConsistencyLevel.ONE);
				}
			}
		}
		if (VERBOSE)
			System.out.println(CDC + "Tracking table prepared!");
	}
}
