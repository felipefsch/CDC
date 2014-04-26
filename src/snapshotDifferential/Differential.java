package snapshotDifferential;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Utils;

/**
 * @author felipe
 * 
 * This compares two snapshots and computes the differential.
 * It has one mapper for each snapshot (that must be stored in two different HDFS paths), which tags the source of each value and send them
 * to the reducer, which will receive the values grouped by the keys generated by the mappers and compare them to extract
 * the changed data.
 * The final result is stored in the desired HDFS path.
 *
 * The input snapshots paths are based on the snapshot_hdfs_path in .properties.
 *
 */
public class Differential extends Configured implements Tool {
	// Necessary configuration files of hadoop
	private static String CORE_SITE = "/home/felipe/hadoop/conf/core-site.xml";
	private static String HDFS_SITE = "/home/felipe/hadoop/conf/hdfs-site.xml";
	
	// Paths to use
	private static String OLD_SNAPSHOT = "hdfs://localhost:9000/snapshot/3";
	private static String NEW_SNAPSHOT = "hdfs://localhost:9000/snapshot/4";
	private static String OUTPUT_PATH = "hdfs://localhost:9000/snapshot/differentialSuper";
	
	private static Properties prop;
	
	private static boolean VERBOSE = false;
	
	private static String CDC = utils.Utils.CDC;
	private static String ERROR = utils.Utils.ERROR;
	
    public static void main(String... args) throws Exception
    {    	
    	String path = args.length > 0 ? args[0] : "./CDC.properties";

    	VERBOSE = args.length > 1 && args[1].equals("-verbose") ? true : false;
    	
    	/*if (VERBOSE)	    	
    		System.out.println(CDC + "Properties loaded from: " + path);*/
    	
    	prop = Utils.getCassandraProp(path);
    	  	
    	CORE_SITE = prop.getProperty("hadoop.core_site");
    	HDFS_SITE = prop.getProperty("hadoop.hdfs_site");

    	OLD_SNAPSHOT = prop.getProperty("snapshot_differential.snapshot_hdfs_path")
    			+ prop.getProperty("snapshot_differential.differential_old_snapshot");
    	NEW_SNAPSHOT = prop.getProperty("snapshot_differential.snapshot_hdfs_path")
    			+ prop.getProperty("snapshot_differential.differential_new_snapshot");
    	OUTPUT_PATH = prop.getProperty("snapshot_differential.differential_output_path");
    	
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new Differential(), args);
        System.exit(0);
    }
        
    @SuppressWarnings("deprecation")
	public static class MapperOldSnapshot extends MapReduceBase implements Mapper<LongWritable, Text, Text, TextPair> {
    
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, TextPair> output, Reporter reporter) throws IOException {
			String outKey = null;
			String outVal = null;
			
			StringTokenizer tokens = new StringTokenizer(value.toString());
			while (tokens.hasMoreTokens()) {
				if (outKey == null)
					outKey = tokens.nextToken();
				else
					outVal = tokens.nextToken();
			}
			
			TextPair outValue = new TextPair(new Text("0"), new Text(outVal));
			
			if (VERBOSE)
				System.out.println(CDC + "[OLD] " + "key: " + outKey + " value: " + outVal);
			
			output.collect(new Text(outKey), outValue);
		}
    }
    
    @SuppressWarnings("deprecation")
	public static class MapperNewSnapshot extends MapReduceBase implements Mapper<LongWritable, Text, Text, TextPair> {
    	
    	@Override
    	public void map(LongWritable key, Text value, OutputCollector<Text, TextPair> output, Reporter reporter) throws IOException {
			String outKey = null;
			String outVal = null;
			
			StringTokenizer tokens = new StringTokenizer(value.toString());
			while (tokens.hasMoreTokens()) {
				if (outKey == null)
					outKey = tokens.nextToken();
				else
					outVal = tokens.nextToken();
			}
			
			TextPair outValue = new TextPair(new Text("1"), new Text(outVal));

			if (VERBOSE)
				System.out.println(CDC + "[NEW] " +"key: " + outKey + " value: " + outVal);
			
			output.collect(new Text(outKey), outValue);
    	}
    }
    @SuppressWarnings("deprecation")
	public static class DifferentialReducer extends MapReduceBase implements Reducer<Text, TextPair, Text, Text>{
		
		@Override
		public void reduce(Text key, Iterator<TextPair> values,OutputCollector<Text, Text> output, Reporter arg3) throws IOException {
			
			Text oldValue = null;
			Text newValue = null;
			
    		while (values.hasNext()) {

    			TextPair val = values.next();
    			Text value = val.getSecond();
    			Text source = val.getFirst();
    			
    			if (source.toString().equals("0")) {
    				oldValue = new Text(value);    				
    			}
    			else if (source.toString().equals("1")) {
    				newValue = new Text(value);
    			}
    		}
    		
    		// Definition of which information to be stored
    		Text outputValue = new Text();    		
    		
    		// Inserted standard column
    		if (oldValue == null && newValue != null) {
    			//System.out.println("key: " + key.toString() + " inserted value: " + secondValue);
    			//key = new Text("ins/" + key.toString());
    			outputValue = new Text(newValue + "/ins");
    		}
    		// Deleted standard column
    		else if (oldValue != null && newValue == null) {
    			//System.out.println("key: " + key.toString() + " deleted value: " + firstValue);
    			//key = new Text("del/" + key.toString());
    			outputValue = new Text(oldValue + "/del");
    		}
    		// Updated standard column
    		else if (oldValue != null && newValue != null && !oldValue.toString().equals(newValue.toString())) {
    			//System.out.println("key: " + key.toString() + " value: " + firstValue + " updated to: " + secondValue);
    			//key = new Text("upd/" + key.toString());
    			outputValue = new Text(newValue + "/upd");
    		}
    		// If it's not in one of the preview cases, nothing happens.
    		else {
    			key = null;
    			outputValue = null;
    		}    
    		
    		if (VERBOSE)
    			System.out.println(CDC + "key:" + key + " value: " + outputValue);
    		
    		output.collect(key, outputValue);    		
		}
    }

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] arg0) throws Exception {

		getConf().addResource(new Path(CORE_SITE));
		getConf().addResource(new Path(HDFS_SITE));
		
		JobConf job = new JobConf(getConf());
		
		job.setJarByClass(Differential.class);
		job.setJobName("Differential");
		
		job.setInputFormat(TextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapperOldSnapshot.class);
		job.setReducerClass(DifferentialReducer.class);
		
		FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(OLD_SNAPSHOT)) && fs.exists(new Path(NEW_SNAPSHOT)))
        {        	  
      	  MultipleInputs.addInputPath(job, new Path(OLD_SNAPSHOT),
        		  TextInputFormat.class, MapperOldSnapshot.class);
	          
          MultipleInputs.addInputPath(job, new Path(NEW_SNAPSHOT),
        		  TextInputFormat.class, MapperNewSnapshot.class);
      	  
          // Output path for the Job	          
          if (fs.exists(new Path(OUTPUT_PATH))){
        	  fs.delete(new Path(OUTPUT_PATH), true);          	  
          }
          FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        }
        else {
        	System.out.println(CDC + ERROR + "Old or New snapshot paths does not exists. Job will be terminated!");
        	return 0;
        }
        
		JobClient.runJob(job);
		
		return 0;
	}
}