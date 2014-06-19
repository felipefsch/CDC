package auditColumn;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import utils.Utils;

public class AuditColumn extends Configured implements Tool{
/*------------------------------Necessary configuration-----------------------------------------------*/
	
	// Necessary configuration files of hadoop
	private static String CORE_SITE = "/opt/hadoop-0.20.2/conf/core-site.xml";
	private static String HDFS_SITE = "/opt/hadoop-0.20.2/conf/hdfs-site.xml";	
	
	// Necessary Cassandra configurations (default already setted up)
	private static String RPC_PORT = "9160";
	private static String ADDRESS = "localhost";
	
	// Desired HDFS output path
	private static String OUTPUT_PATH = "hdfs://localhost:9000/auditColumnDifferential/0/";
	
    // SuperColumn names or Column names (depending on your IS_SUPER)
    private static String KEYSPACE = "School";
    private static String COLUMN_FAMILY = "AuditColumn";
    private static boolean IS_SUPER = true;
	private static List<String> COLUMNS = new ArrayList<String>(
    											Arrays.asList("SuperColumn"));
	
	private static long LAST_CYCLE = 0;
/*-----------------------------------------------------------------------------------------------------*/
	
	// Don't change this field, it's used to setup the column names before the Mapper
    private static final String CONF_COLUMN_NAME = "columnname";
    
    private static Properties prop;
    
    private static String PROP_PATH = "./CDC.properties";
    
    private static boolean VERBOSE = false;
    
    private static String CDC = Utils.CDC;
    
    public static void main(String... args) throws Exception
    {
    	
    	PROP_PATH = args.length > 0 ? args[0] : "./CDC.properties";
    	
    	VERBOSE = args.length > 1 && args[1].equals("-verbose") ? true : false;
    	
    	prop = Utils.getCassandraProp(PROP_PATH);
    		    	    	
    	CORE_SITE = prop.getProperty("hadoop.core_site");
    	HDFS_SITE = prop.getProperty("hadoop.hdfs_site");
    	
    	RPC_PORT = prop.getProperty("cassandra.rpc_port");
    	ADDRESS = prop.getProperty("cassandra.address");
    	
    	KEYSPACE = prop.getProperty("cassandra.keyspace");
    	COLUMN_FAMILY = prop.getProperty("cassandra.column_family");
    	OUTPUT_PATH = prop.getProperty("audit_column.hdfs_output_path");
    	IS_SUPER = prop.getProperty("cassandra.is_super_cf").equals("true") ? true : false;
    	
    	LAST_CYCLE = Utils.getLastCycle(PROP_PATH);
    	if (IS_SUPER){
    		COLUMNS = Utils.superColumnNames(PROP_PATH);
    	}
    	else {
    		COLUMNS = Utils.standardColumnNames(PROP_PATH);
    	}
    	
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new AuditColumn(), args);
//        System.exit(0);
    }

    /** 
     * @author felipe
     *	This Mapper creates keys and respective values for standard columns.
     *
     */
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
        		String teste = tokens.nextToken().toString();        		
        		if (!teste.matches("null")){        			
        			listColumnNames.add(teste);
        		}
        	}

        }        

        @Override
        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {          	
        	Iterator<String> iterator = listColumnNames.iterator();
        	
        	while (iterator.hasNext()) {
        		
        		String colName = iterator.next();
        		        		
                IColumn column = columns.get(ByteBufferUtil.bytes(colName));
                
                if (column == null)
                    continue;
                
                String columnValue = ByteBufferUtil.string(column.value());                        
                String keyName = ByteBufferUtil.string(key);          
                String columnTimestamp = "" + column.timestamp();
                //String colName = ByteBufferUtil.string(column.name());
                
                if (colName.startsWith("audit:")) {
                	colName = colName.substring(colName.indexOf(":") + 1);
                	columnValue = "audit:" + columnValue;
                }
                            
                // Timestamp not in the key part but in value part
                // Key designed to follow the same pattern as the other approaches.
                outKeyName.set(keyName + "/null/" + colName);
                outColumnValue.set(columnValue + "-" + columnTimestamp);
                
                context.write(outKeyName, outColumnValue);
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
        		String teste = tokens.nextToken().toString();        		
        		if (!teste.matches("null")){        			
        			listColumnNames.add(teste);
        		}
        	}

        }        

        @Override
        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {          	
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
                	String subColumnName = ByteBufferUtil.string(subColumn.name());
                	String subColumnValue = ByteBufferUtil.string(subColumn.value());
                	String subColumnTimestamp = "" + subColumn.timestamp();
                	
                    if (subColumnName.startsWith("audit:")) {
                    	subColumnName = subColumnName.substring(subColumnName.indexOf(":") + 1);
                    	subColumnValue = "audit:" + subColumnValue;
                    }
                	
                	// Key and value designed to follow the same pattern as the other approaches
                    outKeyName.set(keyName + "/" + superColumnName + "/" + subColumnName);
                    outColumnValue.set(subColumnValue + "-" + subColumnTimestamp);
                    
//                    System.out.println(outKeyName.toString() + "  " + outColumnValue.toString());                    
                    
                    context.write(outKeyName, outColumnValue);
                }
        	}        	           
        }
    }
    
    public static class AnalysisReducer extends Reducer<Text, Text, Text, Text> {
    	
    	@Override
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		String strKey = key.toString();
    		String keyName = strKey.substring(0, strKey.indexOf("/"));
    		String superColName = strKey.substring(strKey.indexOf("/") + 1, strKey.lastIndexOf("/"));
    		String subColName = strKey.substring(strKey.lastIndexOf("/") + 1);
    		
    		String effectiveValue = null;
    		long effectiveTimestamp = 0;
    		String auditValue = null;
    		long auditTimestamp = 0;
    		
    		for (Text value : values) {
    			String strValue = value.toString();
    			
    			if (strValue.startsWith("audit:")) {
    				auditValue = strValue.substring(strValue.indexOf(":") + 1, strValue.lastIndexOf("-"));
    				auditTimestamp =  Long.parseLong(strValue.substring(strValue.lastIndexOf("-") + 1));
    			}
    			else {
    				effectiveValue = strValue.substring(0, strValue.lastIndexOf("-"));
    				effectiveTimestamp =  Long.parseLong(strValue.substring(strValue.lastIndexOf("-") + 1));
    			}
    		}
    		
    		String outputKey = "!";
    		String outputValue = "!";
    		
//			// Deletion - I cannot garantee
//			if (effectiveTimestamp == 0) {
//				outputKey = "del/" + KEYSPACE 
//								+ "/" + COLUMN_FAMILY
//								+ "/" + keyName
//								+ "/" + superColName
//								+ "/" + subColName;
//				outputValue = auditValue;
//			}
    		// Update
    		if (effectiveTimestamp > auditTimestamp && effectiveTimestamp > LAST_CYCLE && auditTimestamp < LAST_CYCLE) {
    			outputKey = "upd/" + KEYSPACE 
								+ "/" + COLUMN_FAMILY
								+ "/" + keyName
								+ "/" + superColName
								+ "/" + subColName;
    			outputValue = effectiveValue;
    			
//    			try {
//					insertCassandra(keyName, superColName, superColName, effectiveValue, effectiveTimestamp);
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
    		}
    		// Insertion
    		else if (effectiveTimestamp == auditTimestamp && effectiveTimestamp > LAST_CYCLE) {
    			outputKey = "ins/" + KEYSPACE 
								+ "/" + COLUMN_FAMILY
								+ "/" + keyName
								+ "/" + superColName
								+ "/" + subColName;
				outputValue = effectiveValue;
    		}
    		// Update after Insertion (logically treat as insertion, all happened after last cycle)
    		else if (effectiveTimestamp > auditTimestamp && effectiveTimestamp > LAST_CYCLE && auditTimestamp > LAST_CYCLE) {
    			outputKey = "ins/" + KEYSPACE 
								+ "/" + COLUMN_FAMILY
								+ "/" + keyName
								+ "/" + superColName
								+ "/" + subColName;
				outputValue = effectiveValue;
    		}
    		
    		if (!outputKey.equals("!") && !outputValue.equals("!")) {
    			if (VERBOSE)
        			System.out.println(CDC + outputKey + " " + outputValue);
    			
    			context.write(new Text(outputKey), new Text(outputValue));
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
    	String auditColumn;
    	for (String columnName : COLUMNS) {
    		auditColumn = "audit:" + columnName;
    		columns.add(ByteBufferUtil.bytes(columnName));
    		columns.add(ByteBufferUtil.bytes(auditColumn));
    		allColumnNames = auditColumn + " " + columnName + " " + allColumnNames;
    	}
    	
        getConf().set(CONF_COLUMN_NAME, allColumnNames);                        
        
        getConf().addResource(new Path(CORE_SITE));
        getConf().addResource(new Path(HDFS_SITE));

        Job job = new Job(getConf(), "StoreSnapshot");
        job.setJarByClass(AuditColumn.class);
        
        if (IS_SUPER) {
        	job.setMapperClass(SuperColumnsMapper.class);        	
        }
        else {
        	job.setMapperClass(StandardColumnsMapper.class);
        }

        job.setReducerClass(AnalysisReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class); 
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileSystem fs = FileSystem.get(getConf());
        
        if (fs.exists(new Path(OUTPUT_PATH))){
        	  fs.delete(new Path(OUTPUT_PATH), true);          	  
        }
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.setInputFormatClass(ColumnFamilyInputFormat.class);
        
        ConfigHelper.setRpcPort(job.getConfiguration(), RPC_PORT);
        ConfigHelper.setInitialAddress(job.getConfiguration(), ADDRESS);
        ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
        
        SlicePredicate predicate = new SlicePredicate().setColumn_names(columns);
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
     
        job.waitForCompletion(true);
       
        return 0;
    }
    
    private static void insertCassandra(String key, String superColumnName, String columnName, String value, long timestamp) throws Exception {
		// connecting to Cassandra
        TTransport tr = new TFramedTransport(new TSocket("localhost", 9160));
        TProtocol proto = new TBinaryProtocol(tr);
        try {
	        tr.open();
	        
	        // Cassandra thrift client
	        Cassandra.Client client = new Cassandra.Client(proto);
	        client.set_keyspace(KEYSPACE);
	        
	        ColumnParent parent = new ColumnParent();
	        parent.setColumn_family(COLUMN_FAMILY);
	        
	        if (!superColumnName.equals("null"))
	        	parent.setSuper_column(superColumnName.getBytes("UTF-8"));
	        
	        Column column = new Column();
	        column.setTimestamp(timestamp);

    		column.setValue(value.getBytes("UTF-8"));
    		client.insert(ByteBufferUtil.bytes(key), parent, column, ConsistencyLevel.ONE);
    		
    		columnName = "ins:" + columnName;
    		column.setName(columnName.getBytes("UTF-8"));
    		column.setValue(("ins:" + value).getBytes("UTF-8"));
    		client.insert(ByteBufferUtil.bytes(key), parent, column, ConsistencyLevel.ONE);
        }
        catch (Exception e) {
        	tr.close();
        	System.out.println(e);
        }
        finally {
        	tr.close();
        	System.out.println("Inserted");
        }
    }
 
}
