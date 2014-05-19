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
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
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
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import tableScan.TableScan.StandardColumnsMapper;
import tableScan.TableScan.SuperColumnsMapper;
import utils.*;

public class UpdateTrackingTableMapRed extends Configured implements Tool{
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
		private static List<String> COLUMNS = new ArrayList<String>(
	    											Arrays.asList(Utils.DEL, Utils.INS, Utils.UP_OLD, Utils.UP_NEW));
		
		private static String SOURCE_TABLE = "Grades";
		private static String TRACKING_TABLE = "GradesTracking";
		
		private static long LAST_CYCLE = 0;
		private static long CURRENT_MAINTAINING_CYCLE = 0;
		
		private static boolean VERBOSE = false;
		private static String IS_SUPER = "false";
		
		private static String CDC = utils.Utils.CDC;
	/*-----------------------------------------------------------------------------------------------------*/
		
		// Don't change this field, it's used to setup the column names before the Mapper
	    private static final String CONF_COLUMN_NAME = "columnname";
	    
	    private static Properties prop;
	    
	    public static void main(String... args) throws Exception
	    {	
	    	String path = args.length > 0 ? args[0] : "./CDC.properties";
	    	
	    	VERBOSE = args.length > 1 && args[1].equals("-verbose") ? true : false;
	    	
	    	prop = Utils.getCassandraProp(path);
	    		    	    	
	    	CORE_SITE = prop.getProperty("hadoop.core_site");
	    	HDFS_SITE = prop.getProperty("hadoop.hdfs_site");
	    	
	    	RPC_PORT = prop.getProperty("cassandra.rpc_port");
	    	ADDRESS = prop.getProperty("cassandra.address");
	    	
	    	KEYSPACE = prop.getProperty("cassandra.keyspace");
	    	SOURCE_TABLE = prop.getProperty("cassandra.column_family");
	    	OUTPUT_PATH = prop.getProperty("tracking_table.hdfs_output_path");
	    	
	    	LAST_CYCLE = Integer.parseInt(prop.getProperty("cdc.last_cycle"));
	    	
	    	IS_SUPER = prop.getProperty("cassandra.is_super_cf");
	    	
	    	TRACKING_TABLE = prop.getProperty("tracking_table.column_family");
	    	
	    	CURRENT_MAINTAINING_CYCLE = System.currentTimeMillis() * 1000;
	    	
	    	if (IS_SUPER.equals("true")){
	    		COLUMNS = Utils.superColumnNames(path);
	    	}
	    	else {
	    		COLUMNS = Utils.standardColumnNames(path);
	    	}
	    	
	        // Let ToolRunner handle generic command-line options
	        ToolRunner.run(new Configuration(), new UpdateTrackingTableMapRed(), args);
	    }
	    
	    public static class StandardColumnsMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text>
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
		        	// connecting to Cassandra
		            TTransport tr = new TFramedTransport(new TSocket(ADDRESS, Integer.parseInt(RPC_PORT)));
		            TProtocol proto = new TBinaryProtocol(tr);
		            
		            // Cassandra thrift client
		            Cassandra.Client client = new Cassandra.Client(proto);
		            tr.open();	        	        
		            
		            client.set_keyspace(KEYSPACE);
		        	
		        	Iterator<String> iterator = listColumnNames.iterator();
		        	
		        	String strKey = Utils.toString(key);
		        	
		        	while (iterator.hasNext()) {
		        		
		        		String colName = iterator.next();
		        		
		                IColumn column = columns.get(ByteBufferUtil.bytes(colName));
		                
		                if (column == null)
		                    continue;
		    			
	    				String finalValue = Utils.toString(column.value());
	    				ColumnParent parent = new ColumnParent();
	    				parent.setColumn_family(TRACKING_TABLE);
	    				parent.setSuper_column(Utils.UP_OLD.getBytes("UTF-8"));
	    		        Column c = new Column();//colOrSuperCol.column.name, finalValue.getBytes("UTF-8"), new Clock(colOrSuperCol.column.clock.timestamp));
	    		        
	    		        c.setName(Utils.toByteBuffer(colName));
	    		        c.setValue(finalValue.getBytes("UTF-8"));
	    		        c.setTimestamp(column.timestamp());
	    		        
	    		        client.insert(Utils.toByteBuffer(strKey), parent, c, ConsistencyLevel.ONE);
	    				
	    		        if (VERBOSE) {
	    					System.out.println(CDC + strKey + "/" + colName + "/" + 
	    							finalValue + "/" +
	    							column.timestamp());
	    		        }		    			
		    		}
		        	tr.close();
	        	} catch (Exception e) {
	        		System.err.println(e);
	        	}
	        }
	    }
	    
	    /** 
	     * @author felipe
	     *
	     *	This Mapper creates keys and respective values for super columns.
	     *
	     */
	    public static class SuperColumnsMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text>
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
		        	// connecting to Cassandra
		            TTransport tr = new TFramedTransport(new TSocket(ADDRESS, Integer.parseInt(RPC_PORT)));
		            TProtocol proto = new TBinaryProtocol(tr);
		            
		            // Cassandra thrift client
		            Cassandra.Client client = new Cassandra.Client(proto);
		            tr.open();	        	        
		            
		            client.set_keyspace(KEYSPACE);
		        	
		        	Iterator<String> iterator = listColumnNames.iterator();
		        	
		        	while (iterator.hasNext()) {
		                IColumn column = columns.get(ByteBufferUtil.bytes(iterator.next().toString()));
		                if (column == null)
		                    continue;                
		                
		                String keyName = ByteBufferUtil.string(key);          
		                String superColumnName = ByteBufferUtil.string(column.name());
		                
		                Collection<IColumn> subColumns = column.getSubColumns();
		                Iterator<IColumn> subColumnIterator = subColumns.iterator();
		                while (subColumnIterator.hasNext()) {	                	
			                	IColumn subColumn = subColumnIterator.next();
			                	
			                	String finalValue = Utils.toString(column.value());
								ColumnParent parent = new ColumnParent();
								parent.setColumn_family(TRACKING_TABLE);
								parent.setSuper_column(Utils.UP_OLD.getBytes(Utils.UTF_8));
								String finalColName = superColumnName + "/" + subColumn.name();  
						        Column c = new Column();
						        
						        c.setName(Utils.toByteBuffer(finalColName));
						        c.setValue(finalValue.getBytes(Utils.UTF_8));
						        c.setTimestamp(subColumn.timestamp());
						        
						        client.insert(Utils.toByteBuffer(keyName), parent, c, ConsistencyLevel.ONE);
						        
			    		        if (VERBOSE) {
			    					System.out.println(CDC + keyName + "/" + finalColName + "/" + 
			    							finalValue + "/" +
			    							column.timestamp());
			    		        }
		                	}
		                }
		                tr.close();
		        	} catch (Exception e) {
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
	    	
	    	startTrackingTable();
	    	
	    	// Create a job for each Column of Cassandra    	
	    	String allColumnNames = null;
	    	
	    	List<ByteBuffer> columns = new ArrayList<ByteBuffer>();
	    		    	
	    	// create string with all column names and list with bytes of all column names
	    	for (String columnName : COLUMNS) {
	    		columns.add(ByteBufferUtil.bytes(columnName));
	    		allColumnNames = columnName + " " + allColumnNames;
	    	}
	    	
	        getConf().set(CONF_COLUMN_NAME, allColumnNames);                        
	        
	        getConf().addResource(new Path(CORE_SITE));
	        getConf().addResource(new Path(HDFS_SITE));

	        Job job = new Job(getConf(), UpdateTrackingTableMapRed.class.getName());
	        job.setJarByClass(UpdateTrackingTableMapRed.class);
	        
	        if (IS_SUPER.equals("true")) {
	        	if (VERBOSE)
	        		System.out.println(CDC + "Setting mapper for Super Column Family");
	        	
	        	job.setMapperClass(SuperColumnsMapper.class);        	
	        }
	        else {
	        	if (VERBOSE)
	        		System.out.println(CDC + "Setting mapper for Standard Column Family");
	        	
	        	job.setMapperClass(StandardColumnsMapper.class);
	        }  	
	        
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
	        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, SOURCE_TABLE);
	        
	        SlicePredicate predicate = new SlicePredicate().setColumn_names(columns);
	        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
	        
	        if (VERBOSE)
	        	System.out.println(CDC + "Waiting to be completed");
	        
	        job.waitForCompletion(true);
	       
	        return 0;
	    } 	

		private static void startTrackingTable() throws InvalidRequestException, TException {
			// connecting to Cassandra
	        TTransport tr = new TFramedTransport(new TSocket(ADDRESS, Integer.parseInt(RPC_PORT)));
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

	}

