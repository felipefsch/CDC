package logBased;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Utils;

@SuppressWarnings("deprecation")
public class LogBased extends Configured implements Tool{

	private static String CASSANDRA_YAML = "file:///opt/apache-cassandra-1.0.9/conf/cassandra.yaml";
	private static String LOG4J_TOOLS = "file:///opt/apache-cassandra-1.0.9/conf/log4j-tools.properties";
	
	private static String CORE_SITE = "/opt/hadoop-0.20.2/conf/core-site.xml";
	private static String HDFS_SITE = "/opt/hadoop-0.20.2/conf/hdfs-site.xml";
	
	private static String OUTPUT_PATH = "hdfs://localhost:9000/LogBased/differential";
	
	private static String RPC_PORT = "9160";
	private static String ADDRESS = "localhost";
	
	private static Properties prop;
	
	private static boolean VERBOSE = false;
	
	private static String CDC = utils.Utils.CDC;
	private static String MAPPER = utils.Utils.MAPPER;
	private static String REDUCER = utils.Utils.REDUCER;
	
    public static void main(String... args) throws Exception
    {
    	String path = args.length > 0 ? args[0] : "./CDC.properties";
    	
    	VERBOSE = args.length > 1 && args[1].equals("-verbose") ? true : false;
    	
    	prop = Utils.getCassandraProp(path);
    		    	    	
    	CORE_SITE = prop.getProperty("hadoop.core_site");
    	HDFS_SITE = prop.getProperty("hadoop.hdfs_site");
    	
    	RPC_PORT = prop.getProperty("cassandra.rpc_port");
    	ADDRESS = prop.getProperty("cassandra.address");
    	
    	CASSANDRA_YAML = prop.getProperty("cassandra.yaml");
    	LOG4J_TOOLS = prop.getProperty("cassandra.log4j_tools");
    	
    	OUTPUT_PATH = prop.getProperty("log_based.hdfs_output_path");
    	
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new LogBased(), args);
//        System.exit(0);
    }
	
	/**
	 * The mapper generates keys (as keyspace/columnFamily/key) and values (as superColumn/column/value/timestamp, where
	 * superColumn = "null" for standard column families) from the CommitLogSegments.
	 * 
	 * @author felipe
	 *
	 */
    public static class ReadSegmentsMapper extends MapReduceBase implements Mapper<NullWritable, BytesWritable, Text, Text> {

		@Override
		public void map(NullWritable key, BytesWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			byte[] logRowBytes = value.getBytes();
			InputStream is = new ByteArrayInputStream(logRowBytes);

			DataInputStream dis = new DataInputStream(is);
			
			try {
				LogRow logRow = LogDeserializer.deserialize(dis);
				
				if (logRow.getKeyspace() != null) {
					
					String outputKey = KeyValueGenerator.generateKey(logRow);
					String outputValue = KeyValueGenerator.generateValue(logRow);
					
					if (VERBOSE)
						System.out.println(CDC + MAPPER + "Key: " + outputKey + " value: " + outputValue);
					
					output.collect(new Text(outputKey), new Text(outputValue));					
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * The reducer analyse mutations of all superColumn and columns of the given key, storing the final key-values into HDFS. 
	 * 
	 * @author felipe
	 *
	 */
    public static class AnalysisReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		
		@Override
		public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			List<LogRowValue> listLRV = new ArrayList<LogRowValue>();
			List<LogRowValue> resultingLRV = new ArrayList<LogRowValue>();
			
			// Create LogRowValue objects based on the parsing of input values
			while (values.hasNext()) {
				Text value = values.next();
				
				if (value != null) {
					LogRowValue logRowValue = new LogRowValue();
					logRowValue = KeyValueGenerator.createLRVObject(value);
					
					listLRV.add(logRowValue);
				}
			} 
			
			// Just one mutation, no need of complex analysis
			if (listLRV.size() == 1)
				resultingLRV.add(listLRV.get(0));
			else {
				// Delegate all logic
				resultingLRV = FinalMutationExtractor.extractMutations(listLRV);
			}
			
			String finalKey = null;
			String finalValue = null;

			// Creates and stores the final key for each valid mutation
			for (LogRowValue finalRow : resultingLRV) {
				
				if (finalRow.getSuperColumnName().equals("null")) {
					// key[keyspace/columnFamily/key]
					finalKey = finalRow.getOperation().toString() + "/" + key
										+ "/" + finalRow.getSuperColumnName() 
										+ "/" + finalRow.getColumnName();
					
					finalValue = finalRow.getValue() + "-" + finalRow.getTimestamp();
				}
				else {
					// key[keyspace/columnFamily/key]
					finalKey = finalRow.getOperation().toString() + "/" + key
										+ "/" + finalRow.getSuperColumnName() 
										+ "/" + finalRow.getColumnName();
					
					finalValue = finalRow.getValue() + "-" + finalRow.getTimestamp();					
				}
				
				if (VERBOSE)
					System.out.println(CDC + REDUCER + "Key: " + finalKey + " value: " + finalValue);
				
				output.collect(new Text(finalKey), new Text(finalValue));
			}
		}
    }
	
	@Override
	public int run(String[] arg0) throws Exception {
		// Necessary files for a right configuration of the job
		System.setProperty("cassandra.config", CASSANDRA_YAML);		
		System.setProperty("log4j.configuration", LOG4J_TOOLS);
		getConf().addResource(new Path(CORE_SITE));
		getConf().addResource(new Path(HDFS_SITE));
		
		JobConf job = new JobConf(getConf());
		
		job.setJarByClass(LogBased.class);
		job.setJobName("LogBaseDifferential");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(ReadSegmentsMapper.class);
		job.setReducerClass(AnalysisReducer.class);
		
		job.setInputFormat(SegmentInputFormat.class);
		
		// Set input path as the one with the commit log in cassandra
		String commitLogFolder = "file://" + DatabaseDescriptor.getCommitLogLocation();
		SegmentInputFormat.setInputPaths(job, commitLogFolder);
		
		if (VERBOSE) {
			System.out.println(CDC + "CommitLog file: " + commitLogFolder);
		}
		
        // Output path for the Job	          
		FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(OUTPUT_PATH))){
        	if (VERBOSE)
        		System.out.println(CDC + "Deleting existing output path: " + OUTPUT_PATH);
        	
        	fs.delete(new Path(OUTPUT_PATH), true);          	  
        }
        
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		
		JobClient.runJob(job);
		return 0;
	}
}
