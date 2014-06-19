package trackingTable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.SortedMap;
import java.util.StringTokenizer;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import utils.*;

public class TrackingTableMapRed extends Configured implements Tool{
	/*------------------------------Necessary configuration-----------------------------------------------*/
		
		// Necessary configuration files of hadoop
		private static String CORE_SITE = "/opt/hadoop-0.20.2/conf/core-site.xml";
		private static String HDFS_SITE = "/opt/hadoop-0.20.2/conf/hdfs-site.xml";	
		
		// Necessary Cassandra configurations (default already setted up)
		private static String RPC_PORT = "9160";
		private static String ADDRESS = "localhost";
		
		// Desired HDFS output path (default port is 9000)
		private static String OUTPUT_PATH = "hdfs://localhost:54310/tableScan/0/";
		
		// YCSB default
		private static String KEYSPACE = "usertable";
	    private static String TRACKING_TABLE = "dataTracking";
		private static List<String> SUPER_COLUMNS = new ArrayList<String>(
	    											Arrays.asList(Utils.DEL, Utils.INS, Utils.UP_NEW, Utils.UP_OLD));
		
		private static long LAST_CYCLE = 0;
		private static long CURRENT_MAINTAINING_CYCLE = 0;
		
		private static boolean VERBOSE = false;
		
		private static String CDC = utils.Utils.CDC;
	/*-----------------------------------------------------------------------------------------------------*/
		
		// Don't change this field, it's used to setup the column names before the Mapper
	    private static final String CONF_COLUMN_NAME = "columnname";
	    
	    private static Properties prop;
	    
	    private static String PATH = "./CDC.properties";
	    
	    public static void main(String... args) throws Exception
	    {	
	    	String path = args.length > 0 ? args[0] : "./CDC.properties";
	    	
	    	PATH = path;
	    	
	    	VERBOSE = args.length > 1 && args[1].equals("-verbose") ? true : false;
	    	
	    	prop = Utils.getCassandraProp(path);
	    		    	    	
	    	CORE_SITE = prop.getProperty("hadoop.core_site");
	    	HDFS_SITE = prop.getProperty("hadoop.hdfs_site");
	    	
	    	RPC_PORT = prop.getProperty("cassandra.rpc_port");
	    	ADDRESS = prop.getProperty("cassandra.address");
	    	
	    	KEYSPACE = prop.getProperty("cassandra.keyspace");
	    	TRACKING_TABLE = prop.getProperty("tracking_table.column_family");
	    	OUTPUT_PATH = prop.getProperty("tracking_table.hdfs_output_path");
	    	
	    	LAST_CYCLE = Utils.getLastCycle(path); //Integer.parseInt(prop.getProperty("cdc.last_cycle"));
	    	
	    	System.out.println(LAST_CYCLE);
	    	
	    	CURRENT_MAINTAINING_CYCLE = System.currentTimeMillis() * 1000;	    		    	
	    	
	        // Let ToolRunner handle generic command-line options
	        ToolRunner.run(new Configuration(), new TrackingTableMapRed(), args);
	    }
	    
	    /** 
	     * @author felipe
	     *
	     *	This Mapper creates keys and respective values for super columns.
	     *
	     */
	    public static class TrackingTableMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text>
	    {
	        private Text outKeyName = new Text();
	        private Text outColumnValue = new Text();
	        private List<String> listColumnNames = new ArrayList<String>();
	        private String colNames = null;
	        
	        @Override
	        protected void setup(Context context) throws IOException, InterruptedException {            
	        	colNames = context.getConfiguration().get(CONF_COLUMN_NAME);      

	        	StringTokenizer tokens = new StringTokenizer(colNames);        	
	        	while (tokens.hasMoreTokens()) {        		
	        		String columnName = tokens.nextToken().toString();        		
	        		if (!columnName.matches("null")){        			
	        			listColumnNames.add(columnName);
	        		}
	        	}
	        }        

	        @Override
	        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
	        {  
	        	try {
		        	Iterator<String> iterator = listColumnNames.iterator();
		        	
		        	String outputKey = "";
		    		String outputValue = "";
		        	
		    		String keyName = ByteBufferUtil.string(key);
		    		
		    		if (VERBOSE)
	                	System.out.println(CDC + "Key: " + keyName);		    		
		    		
		        	while (iterator.hasNext()) {
		                IColumn column = columns.get(ByteBufferUtil.bytes(iterator.next().toString()));
		                if (column == null)
		                    continue;                
		                
		                String superColumnName = ByteBufferUtil.string(column.name());
		                
		                Collection<IColumn> subColumns = column.getSubColumns();
		                Iterator<IColumn> subColumnIterator = subColumns.iterator();
		                
						if (VERBOSE)
		                	System.out.println(CDC + "Super Column: " + superColumnName);
		                
		                while (subColumnIterator.hasNext()) {	                	
		                	IColumn subColumn = subColumnIterator.next();
		                	
		                	String subColumnName = ByteBufferUtil.string(subColumn.name());
		                	
		                	Long subColumnTimestamp = subColumn.timestamp();
		                	
							if (VERBOSE)
			                	System.out.println(CDC + "Sub Column: " + subColumnName);
	
		                	if (superColumnName.equals(Utils.UP_NEW)) {	    
	    						/*if (subColumnTimestamp > LAST_CYCLE
	    								&& subColumnTimestamp < CURRENT_MAINTAINING_CYCLE)
	    						{*/					
	    							//insertUpOld(keyName, Utils.toString(subColumn.name()),Utils.toString(subColumn.value()));
	    							//deleteSuperColumn(keyName, Utils.UP_NEW, Utils.toString(subColumn.name()));
	    							
	    							outputKey = outputUpdKey(keyName, subColumnName);
	    							outputValue = outputValue(subColumn.value());
	    							
	    							if (VERBOSE)
					                	System.out.println(CDC + "Key: " + outputKey + " value: " + outputValue);
	    							
	    							context.write(new Text(outputKey), new Text(outputValue));
	    						//}	    					
		    				}
		                	else if (superColumnName.equals(Utils.INS)) {
	    						/*if (subColumnTimestamp > LAST_CYCLE
	    								&& subColumnTimestamp < CURRENT_MAINTAINING_CYCLE)
	    						{*/
	    							//insertUpOld(keyName, Utils.toString(subColumn.name()), Utils.toString(subColumn.value()));
	    							//deleteSuperColumn(keyName, Utils.INS, Utils.toString(subColumn.name()));
	    							
	    							outputKey = outputInsKey(keyName, subColumnName);
	    							outputValue = outputValue(subColumn.value());
	    							
	    							if (VERBOSE)
					                	System.out.println(CDC + "Key: " + outputKey + " value: " + outputValue);
	    							
	    							context.write(new Text(outputKey), new Text(outputValue));
	    						//}		    										
		    				}
		    				else if (superColumnName.equals(Utils.DEL)) {
	    						/*if (subColumnTimestamp > LAST_CYCLE
	    								&& subColumnTimestamp < CURRENT_MAINTAINING_CYCLE)
	    						{*/
	    							outputKey = outputDelKey(keyName, subColumnName);
	    							outputValue = outputValue(subColumn.value());
	    							
	    							//deleteSuperColumn(keyName, Utils.DEL, Utils.toString(subColumn.name()));
	    							//deleteSuperColumn(keyName, Utils.UP_OLD, Utils.toString(subColumn.name()));
	    							
	    							if (VERBOSE)
					                	System.out.println(CDC + "Key: " + outputKey + " value: " + outputValue);
	    							
	    							context.write(new Text(outputKey), new Text(outputValue));
	    						//}				
		    				}	
		                }
		        	}
	        	}
	        	catch (Exception e) {
	        		System.err.println(e);
	        	}
	        }
	    }	   
		
		/**
		 * 
		 *  This configures the Map-Reduce program to create a snapshot of the data stored in Cassandra in to HDFS.
		 *  You need to set the Keyspace and the Column Family of the desired data from Cassandra. If the Column Family
		 *  is of kind Super, you also need to set 'isSuper' to makes it choose the right mapper.
		 *  	
		 */
	    public int run(String[] args) throws Exception
	    {
	    	// Create a job for each Column of Cassandra    	
	    	String allColumnNames = null;
	    	
	    	List<ByteBuffer> columns = new ArrayList<ByteBuffer>();
	    		    	
	    	// create string with all column names and list with bytes of all column names
	    	for (String columnName : SUPER_COLUMNS) {
	    		columns.add(ByteBufferUtil.bytes(columnName));
	    		allColumnNames = columnName + " " + allColumnNames;
	    	}
	    	
	        getConf().set(CONF_COLUMN_NAME, allColumnNames);                        
	        
	        getConf().addResource(new Path(CORE_SITE));
	        getConf().addResource(new Path(HDFS_SITE));

	        Job job = new Job(getConf(), "TableScan");
	        job.setJarByClass(TrackingTableMapRed.class);
	        
        	if (VERBOSE)
        		System.out.println(CDC + "Setting mapper for Super Column Family");
        	
        	job.setMapperClass(TrackingTableMapper.class);  	
	        
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class); 
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
	        FileSystem fs = FileSystem.get(getConf());
	        
	        if (fs.exists(new Path(OUTPUT_PATH))){
	        	if (VERBOSE)
	        		System.out.println(CDC + "Deleting existing output path: " + OUTPUT_PATH);
	        	
	        	fs.delete(new Path(OUTPUT_PATH), true);          	  
	        }
	        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

	        job.setInputFormatClass(ColumnFamilyInputFormat.class);
	        
	        ConfigHelper.setRpcPort(job.getConfiguration(), RPC_PORT);
	        ConfigHelper.setInitialAddress(job.getConfiguration(), ADDRESS);
	        ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
	        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, TRACKING_TABLE);
	        
	        SlicePredicate predicate = new SlicePredicate().setColumn_names(columns);
	        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
	        
	        if (VERBOSE)
	        	System.out.println(CDC + "Waiting to be completed");
	        
	        job.waitForCompletion(true);
	        
	        //if (VERBOSE)
	        //	System.out.println(CDC + "Writing CDC cycle timestamp");
	        
	        //Utils.setLastCycle(PATH, CURRENT_MAINTAINING_CYCLE);
	       
	        return 0;
	    } 
	    
	    private static void deleteSuperColumn(String key, String superColumn, String column) throws Exception {
			// connecting to Cassandra
		    TTransport tr = new TFramedTransport(new TSocket(ADDRESS, Integer.parseInt(RPC_PORT)));
		    TProtocol proto = new TBinaryProtocol(tr);
		    
		    // Cassandra thrift client
		    Cassandra.Client client = new Cassandra.Client(proto);
		    tr.open();	        	        
		    
		    client.set_keyspace(KEYSPACE);
	    	
	    	ColumnPath cp = new ColumnPath();
	        cp.setColumn_family(TRACKING_TABLE);
	        cp.setColumn(column.getBytes(Utils.UTF_8));
	        cp.setSuper_column(superColumn.getBytes(Utils.UTF_8));
	        // Timestamp must be the one of the deletion time indeed?
	        String strKey = key;
	        client.remove(Utils.toByteBuffer(strKey), cp, (System.currentTimeMillis() * 1000), ConsistencyLevel.ALL);
	        
	        tr.close();
	    }	
	    
		private static void insertUpOld(String key, String subColumnName, String value) throws Exception {
			// connecting to Cassandra
		    TTransport tr = new TFramedTransport(new TSocket(ADDRESS, Integer.parseInt(RPC_PORT)));
		    TProtocol proto = new TBinaryProtocol(tr);
		    
		    // Cassandra thrift client
		    Cassandra.Client client = new Cassandra.Client(proto);
		    tr.open();	        	        
		    
		    client.set_keyspace(KEYSPACE);
		    
		    ColumnParent parent = new ColumnParent();
		    parent.setColumn_family(TRACKING_TABLE);
		    parent.setSuper_column(Utils.UP_OLD.getBytes(Utils.UTF_8));
		    
		    Column column = new Column();
		    column.setName(subColumnName.getBytes(Utils.UTF_8));
		    column.setTimestamp(System.currentTimeMillis() * 1000);
		    column.setValue(value.getBytes(Utils.UTF_8));
		    
		    String strKey = key;
		    client.insert(Utils.toByteBuffer(strKey), parent, column, ConsistencyLevel.ONE);
		}
		
		/**
		 * This creates a output key for update operations.
		 */
		private static String outputUpdKey(String key, String subColumnName) {
			return "upd/" + KEYSPACE + "/" + TRACKING_TABLE + "/" + key + "/" + subColumnName;
		}
		
		/**
		 * This creates a output key for deletion operations.
		 */
		private static String outputDelKey(String key, String subColumnName) {
			return "del/" + KEYSPACE + "/" + TRACKING_TABLE + "/" + key + "/" + subColumnName;
		}
		
		/**
		 * This creates a output key for insertion operations.
		 */
		private static String outputInsKey(String key, String subColumnName) {
			return "ins/" + KEYSPACE + "/" + TRACKING_TABLE + "/" + key + "/" + subColumnName;	
		}
		
		/**
		 * This creates the output value.
		 * 
		 * @throws UnsupportedEncodingException 
		 */
		private static String outputValue(ByteBuffer subColumnValue) throws UnsupportedEncodingException {
			String subColVal = Utils.toString(subColumnValue);
			return subColVal;
		}
	}

