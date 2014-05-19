package utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Utils {
	
	public static final String CDC = "[CDC] ";
	public static final String MAPPER = "[MAP] ";
	public static final String REDUCER = "[RED] ";
	public static final String ERROR = "ERROR: ";
	public static final String UP_OLD = "up_old";
	public static final String UP_NEW = "up_new";
	public static final String INS = "ins";
	public static final String DEL = "del";
	public static final String UTF_8 = "UTF-8";
	
	/**
	 * Set into file the last CDC cycle time. File path defined at .properties file. 
	 */
	public static void setLastCycle(String prop_path, long timestamp) {		
		try {
			Properties props = new Properties();
			FileInputStream file = new FileInputStream(prop_path);    	
	    	props.load(file); 
			String path = props.getProperty("cdc.cycle_path");
			
			PrintWriter writer = new PrintWriter(path, "UTF-8");
			writer.println(timestamp);
			writer.close();
		} catch (Exception e) {
			System.err.println(e);
		}		
	}

	/**
	 * Get last CDC cycle time. File path defined at .properties file.
	 * If no path provided, will return cdc.last_cycle property defined at .properties.
	 * 
	 * @return lastCycle
	 */
	public static long getLastCycle(String prop_path) {
		
		String out_path = "";
		long last_cycle = 0;
		
		try {
			Properties props = new Properties();		
			FileInputStream file = new FileInputStream(prop_path);    	
	    	props.load(file); 
	    	
	    	String path = props.getProperty("cdc.cycle_path");
			
		    if (path.equals(""))
				last_cycle =  Long.parseLong(props.getProperty("cdc.last_cycle"));
	    	
			BufferedReader br = new BufferedReader(new FileReader(path));
		    try {
		        String line = br.readLine();
		        last_cycle = Long.parseLong(line);
		    } 
		    catch (Exception e) {
		    	System.err.println(e);
		    }
		    finally {
		        br.close();
		    }
		}
		catch (Exception e) {
			System.err.println(e);
		}
		return last_cycle;		
	}
	
	public static Properties getCassandraProp(String... prop_path) throws Exception {	    	
        String path = prop_path.length > 0 ? prop_path[0] : "./CDC.properties";		
    	Properties props = new Properties();    	
    	FileInputStream file = new FileInputStream(path);    	
    	props.load(file);    	
    	return props;
    }	
    
    public static ByteBuffer toByteBuffer(String value) throws UnsupportedEncodingException
    {
        return ByteBuffer.wrap(value.getBytes("UTF-8"));
    }
    	        
    public static String toString(ByteBuffer buffer) throws UnsupportedEncodingException
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, "UTF-8");
    }
    
    /**
     * This returns a list with all standard column names based on given .properties file.
     * Example: with column_prefix as 'col' and num_colmns = '2' will generate a list with:
     * -(col0, col1)
     * 
     * @param propPath
     * @return colNames
     * @throws Exception
     */
    public static List<String> standardColumnNames(String... propPath) throws Exception {
    	String path = propPath.length > 0 ? propPath[0] : "./CDC.properties";
    	Properties prop = new Properties();
    	prop = getCassandraProp(path);
    	
    	String colPrefix = prop.getProperty("cassandra.column_prefix");
    	int numCols = Integer.parseInt(prop.getProperty("cassandra.num_columns"));
    	
    	ArrayList<String> colNames = new ArrayList<String>();
    	
    	for (int i = 0; i < numCols; i++) {
    		colNames.add(colPrefix + i);    		
    	}
    	
    	return colNames;
    }
    
    /**
     * This returns a list with all super column names based on given .properties file.
     * Example: with super_column_prefix as 'sup' and num_super_columns = '2' will generate a list with:
     * -(sup0, sup1)
     * 
     * @param propPath
     * @return colNames
     * @throws Exception
     */
    public static List<String> superColumnNames(String... propPath) throws Exception {
    	String path = propPath.length > 0 ? propPath[0] : "./CDC.properties";
    	Properties prop = new Properties();
    	prop = getCassandraProp(path);
    	
    	String colPrefix = prop.getProperty("cassandra.super_column_prefix");
    	int numCols = Integer.parseInt(prop.getProperty("cassandra.num_super_columns"));
    	
    	ArrayList<String> colNames = new ArrayList<String>();
    	
    	for (int i = 0; i < numCols; i++) {
    		colNames.add(colPrefix + i);    		
    	}
    	
    	return colNames;
    }   
    
    public static Cassandra.Client getCassandraClient(String... propPath) throws Exception {
    	
    	String path = propPath.length > 0 ? propPath[0] : "./CDC.properties";
    	Properties prop = new Properties();
    	prop = getCassandraProp(path);
    	
		// connecting to Cassandra
    	int rpc_port = Integer.parseInt(prop.getProperty("cassandra.rpc_port"));
        TTransport tr = new TFramedTransport(new TSocket(prop.getProperty("cassandra.address"), rpc_port));
        TProtocol proto = new TBinaryProtocol(tr);
        try {
	        tr.open();
	        
	        // Cassandra thrift client
	        Cassandra.Client client = new Cassandra.Client(proto);	        
	        return client;
        }
        catch (Exception e) {
        	System.out.println(CDC + "Could not connect to Cassandra.");
        	System.out.println(CDC + e);
        	return null;
        }        
    }
}
