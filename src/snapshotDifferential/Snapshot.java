package snapshotDifferential;

import java.io.IOException;
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
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Utils;

/** 
 * @author felipe
 * 
 * This gets a snapshot from Cassandra and stores into HDFS.
 * The data is stored in HDFS OUTPUT_PATH and each column has their own sub-folder.
 * In this sub-folder the Keys and Values are write in standard file named part-r-00000.
 * You must change the KEYSPACE and COLUMN FAMILY for those you want to get the snapshot.
 * 'isSuper' must be TRUE for Super Column Families and false otherwise.
 * You also need to set list 'columns' with the names of the columns you
 * want to take the snapshot.
 * 
 * This map-reduce program does not needs a reduce phase, just a map phase is enough.
 */
public class Snapshot extends Configured implements Tool
{
/*------------------------------Necessary configuration-----------------------------------------------*/
	
	// Necessary configuration files of hadoop
	private static String CORE_SITE = "/home/felipe/hadoop/conf/core-site.xml";
	private static String HDFS_SITE = "/home/felipe/hadoop/conf/hdfs-site.xml";	
	
	// Necessary Cassandra configurations (default already setted up)
	private static String RPC_PORT = "9160";
	private static String ADDRESS = "localhost";
	
	// Desired HDFS output path
	private static String OUTPUT_PATH = "hdfs://localhost:9000/snapshot/5";
	
    // SuperColumn names or Column names (depending on your IS_SUPER)
    private static String KEYSPACE = "School";
    private static String COLUMN_FAMILY = "Salaries";
    private static String IS_SUPER = "false";
	private static List<String> COLUMNS = new ArrayList<String>(
    											Arrays.asList("Docent", "Employees", "Cleaning"));
/*-----------------------------------------------------------------------------------------------------*/
	
	// Don't change this field, it's used to setup the column names before the Mapper
    private static final String CONF_COLUMN_NAME = "columnname";
    
    private static Properties prop;
    
    private static boolean VERBOSE = false;
    
    private static String CDC = utils.Utils.CDC;
    
    public static void main(String... args) throws Exception
    {
    	String path = args.length > 0 ? args[0] : "./CDC.properties";

    	VERBOSE = args.length > 1 && args[1].equals("-verbose") ? true : false;
    	
    	/*if (VERBOSE)	    	
    		System.out.println(CDC + "Properties loaded from: " + path);*/
    	
    	prop = Utils.getCassandraProp(path);
    	    	   	
    	CORE_SITE = prop.getProperty("hadoop.core_site");
    	HDFS_SITE = prop.getProperty("hadoop.hdfs_site");
    	
    	RPC_PORT = prop.getProperty("cassandra.rpc_port");
    	ADDRESS = prop.getProperty("cassandra.address");
    	
    	KEYSPACE = prop.getProperty("cassandra.keyspace");
    	COLUMN_FAMILY = prop.getProperty("cassandra.column_family");
    	OUTPUT_PATH = prop.getProperty("snapshot_differential.snapshot_hdfs_path")
    			+ prop.getProperty("snapshot_differential.snapshot_number");
    	IS_SUPER = prop.getProperty("cassandra.is_super_cf");
    	
    	if (IS_SUPER.equals("true")){
    		COLUMNS = Utils.superColumnNames(path);
    	}
    	else {
    		COLUMNS = Utils.standardColumnNames(path);
    	}
    	
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new Snapshot(), args);
        System.exit(0);
    }

    /** 
     * @author felipe
     *	This Mapper creates keys and respective values for standard columns.
     *
     */
    public static class StandardColumnStoreMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text>
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
                String columnName = ByteBufferUtil.string(column.name());
                            
                // Timestamp not in the key part but in value part
                // Key designed to follow the same pattern as the other approaches.
                outKeyName.set(KEYSPACE + "/" + COLUMN_FAMILY + "/" + keyName + "/null/" + columnName);
                outColumnValue.set(columnTimestamp + "/" + columnValue);
                
                if (VERBOSE)
                	System.out.println(CDC + "Key: " + outKeyName.toString() + " value: " + outColumnValue.toString());
                
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
    public static class SuperColumnStoreMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text>
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
                	
                	// Key and value designed to follow the same pattern as the other approaches
                    outKeyName.set(KEYSPACE + "/" + COLUMN_FAMILY + "/" + keyName + "/" + superColumnName + "/" + subColumnName);
                    outColumnValue.set(subColumnTimestamp+ "/" + subColumnValue);
                    
                    if (VERBOSE)
	                	System.out.println(CDC + "Key: " + outKeyName.toString() + " value: " + outColumnValue.toString());
                    
                    context.write(outKeyName, outColumnValue);
                }
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
    	for (String columnName : COLUMNS) {
    		columns.add(ByteBufferUtil.bytes(columnName));
    		allColumnNames = columnName + " " + allColumnNames;
    	}
    	
        getConf().set(CONF_COLUMN_NAME, allColumnNames);                        
        
        getConf().addResource(new Path(CORE_SITE));
        getConf().addResource(new Path(HDFS_SITE));

        Job job = new Job(getConf(), "Snapshot");
        job.setJarByClass(Snapshot.class);
        
        if (IS_SUPER.equals("true")) {
        	if (VERBOSE)
        		System.out.println(CDC + "Setting mapper for Super Column Family");
        	
        	job.setMapperClass(SuperColumnStoreMapper.class);
        }
        else {
        	if (VERBOSE)
        		System.out.println(CDC + "Setting mapper for Standard Column Family");
        	
        	job.setMapperClass(StandardColumnStoreMapper.class);
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
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
        
        SlicePredicate predicate = new SlicePredicate().setColumn_names(columns);
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
     
        if (VERBOSE)
        	System.out.println(CDC + "Waiting to be completed");
        
        job.waitForCompletion(true);
       
        return 0;
    }        
}