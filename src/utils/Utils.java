package utils;

import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Utils {
	
	public static final String CDC = "[CDC] ";
	public static final String ERROR = "ERROR: ";
	
	
	/**
	 * Set into file at current folder the last CDC cycle time. 
	 */
	public static void setLastCycle() {
		
	}

	/**
	 * Get last cycle time from current folder file.
	 * 
	 * @return lastCycle
	 */
	public static long getLastCycle() {
				
		return 0;
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
}
