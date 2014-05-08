package utils;

import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Export SSTables to JSON format. Is not useful anymore but I keep it here.
 */
public class SSTable2HDFS
{
    private static final String CORE_SITE = "/home/felipe/hadoop/conf/core-site.xml";
    private static final String HDFS_SITE = "/home/felipe/hadoop/conf/hdfs-site.xml";
    
    private static final String CASSANDRA_YAML = "file:///home/felipe/cassandra/conf/cassandra.yaml";
    private static final String LOG4J_PROPERTIES = "file:///home/felipe/cassandra/conf/log4j-tools.properties";
    
    /**
     * This returns a FileSystem.
     * 
     * @return FileSystem
     * @throws Exception
     */
    
    public static FileSystem getHDFS() throws Exception {
    	Configuration conf = new Configuration();
    	
    	conf.addResource(new Path(CORE_SITE));
        conf.addResource(new Path(HDFS_SITE));
    	
    	FileSystem hdfs = FileSystem.get(conf);
    	
        return hdfs;
    }
    
    /**
     * This convert a hexadecimal value to string.
     * 
     * @param hex
     * @return string
     */
    public static String convertHexToString(String hex){    	 
  	  StringBuilder sb = new StringBuilder();
  	  
  	  StringBuilder temp = new StringBuilder();

  	  for( int i=0; i<hex.length()-1; i+=2 ){   
  	      //grab the hex in pairs
  	      String output = hex.substring(i, (i + 2));
  	      //convert hex to decimal
  	      int decimal = Integer.parseInt(output, 16);
  	      //convert the decimal to character
  	      sb.append((char)decimal);
   
  	      temp.append(decimal);
  	  }   
  	  return sb.toString();
    }    
    
    /**
     * This opens a list of SSTables and stores in HDFS as key/value pairs
     *  
     * @param ssTables list of SSTable paths 
     * @param outs
     * @throws Exception 
     */
    static void export(List<String> ssTables, String outPath) throws Exception
    {    	
    	// Connect in HDFS
	    FileSystem hdfs = getHDFS();
	    	
	    // Output file Path
	    Path path = new Path(outPath);
        
        FSDataOutputStream dos = hdfs.create(path);
    	        
    	Iterator iterator = ssTables.iterator();

    	// iterate in all given SSTables 
    	while (iterator.hasNext()){
    		String ssTable = iterator.next().toString();
    		
    		SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(ssTable));
    		
    		SSTableScanner scanner = reader.getDirectScanner();
    		
	    	SSTableIdentityIterator row;
	
	        // collecting keys to export
	        while (scanner.hasNext())
	        {
	            row = (SSTableIdentityIterator) scanner.next();
	
	            String currentKey = bytesToHex(row.getKey().key);
	            
	            ColumnFamily columnFamily = row.getColumnFamily();
	            boolean isSuperCF = columnFamily.isSuper();                        
	            AbstractType comparator = columnFamily.getComparator();
	            
	            if (isSuperCF)
	            {          	           	
	            	// Iterate in Super Columns
	                while (row.hasNext())                	
	                {
	                    IColumn superColumn = row.next();                                                            
	                    
	                    if (superColumn.isMarkedForDelete()) {
	                    	// Key of Deleted Super Column is a compose of values su/Key/SuperColumn/Timestamp
	                		String delSuperColKey = "su/" + 
	                								convertHexToString(currentKey) + "/" +
	                								convertHexToString(comparator.getString(superColumn.name())) + "/" +
	                								superColumn.getMarkedForDeleteAt();
	                		
	                		String delSuperColValue = null;
	                		
	                		//System.out.println("\nkey: " + delSuperColKey + " value: " + delSuperColValue);
	                		
	                		dos.writeBytes(delSuperColKey + ":" + delSuperColValue + "\n");
	                    }
	                    else {                 
	                    	
	                    	Iterator<IColumn> colIterator = superColumn.getSubColumns().iterator();
	                    	
	                    	while (colIterator.hasNext()) {                    		
	                    		IColumn subColumn = colIterator.next();
	                    		
	                    		if (subColumn.isMarkedForDelete()) {
		                    		// Key of Deleted Sub Column is a compose of values su/Key/SuperColumn/SuperColTimestamp/Column/Timestamp/InsertOrDelete
		                    		String superColKey = "su/" +
		                    								convertHexToString(currentKey) + "/" +
		                    								convertHexToString(comparator.getString(superColumn.name())) + "/" +
		                    								superColumn.getMarkedForDeleteAt() + "/" +
		                    								convertHexToString(comparator.getString(subColumn.name())) + "/" +
		                    								subColumn.timestamp()  + "/" +
		                    								subColumn.isMarkedForDelete();
		                    		
		                    		String superColValue = null;
		
		                    		//System.out.println("\nkey: " + superColKey + " value: " + superColValue);
		                    		
		                    		dos.writeBytes(superColKey + ":" + superColValue + "\n");
	                    		}
	                    		else {
		                    		// Key of each Sub Column is a compose of values su/Key/SuperColumn/SuperColTimestamp/Column/Timestamp/isMarketForDelete
		                    		String superColKey = "su/" +
		                    								convertHexToString(currentKey) + "/" +
		                    								convertHexToString(comparator.getString(superColumn.name())) + "/" +
		                    								superColumn.getMarkedForDeleteAt() + "/" +
		                    								convertHexToString(comparator.getString(subColumn.name())) + "/" +
		                    								subColumn.timestamp()  + "/" +
		                    								superColumn.isMarkedForDelete();
		                    		
		                    		String superColValue = convertHexToString(comparator.getString(subColumn.value()));
		
		                    		//System.out.println("\nkey: " + superColKey + " value: " + superColValue);
		                    		
		                    		dos.writeBytes(superColKey + ":" + superColValue + "\n");
	                    		}
	                    	}
	                    }
	                }
	            }
	            else
	            {
	            	while (row.hasNext()) {
	            		IColumn column = row.next();
	            		
	            		if (column.isMarkedForDelete()) {
	            			// Key of Deleted Standard Column is a compose of values st/Key/Column/Timestamp/isMarketForDelete
	            			String standardColKey = "st/" +
	            										convertHexToString(currentKey) + "/" +
	            										convertHexToString(comparator.getString(column.name())) + "/" +
	            										column.getMarkedForDeleteAt() + "/" +
	            										column.isMarkedForDelete();
	            			
	            			String standardColValue = null;
	
	            			//System.out.println("\nkey: " + standardColKey + " value: " + standardColValue);
	            			
	            			dos.writeBytes(standardColKey + ":" + standardColValue + "\n");
	            		}
	            		else {
	            			// Key of Standard Column is a compose of values st/Key/Column/Timestamp/isMarketForDelete
	            			String standardColKey = "st/" +
	            										convertHexToString(currentKey) + "/" +
	            										convertHexToString(comparator.getString(column.name())) + "/" +
	            										column.timestamp() + "/" +
	            										column.isMarkedForDelete();
	            			
	            			String standardColValue = convertHexToString(comparator.getString(column.value()));
	
	            			//System.out.println("\nkey: " + standardColKey + " value: " + standardColValue);
	            			
	            			dos.writeBytes(standardColKey + ":" + standardColValue + "\n");
	            		}
	            	}       
	            }
	        }	        
	        scanner.close();
    	}    	
        dos.close();
    }

    /**
     * This stores the key/value pairs in HDFS.
     * 
     * @param dst destination path
     * @param input is the input file that contains all key/value pairs of the SSTable
     * 
     * @throws Exception
     */
    public static void storeInHDFS() throws Exception {    	    	

    	Configuration conf = new Configuration();
    	conf.addResource(new Path(CORE_SITE));
        conf.addResource(new Path(HDFS_SITE));
    	
    	FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path("/testfile");
        
        //writing
        FSDataOutputStream dos = hdfs.create(path);
        dos.close();    	
    }    
    
    /**
     * export the contents of the SSTable to HDFS.
     *  
     * @param args command lines arguments
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception
    {
        List<String> ssTables = new ArrayList<String>();
    	
    	ssTables.add("/home/felipe/cassandra/storage/data/snapshot/snapshot-hc-5-Data.db");
    	ssTables.add("/home/felipe/cassandra/storage/data/snapshot/snapshot-hc-6-Data.db");
    	ssTables.add("/home/felipe/cassandra/storage/data/snapshot/supersnapshot-hc-9-Data.db");
    	ssTables.add("/home/felipe/cassandra/storage/data/snapshot/supersnapshot-hc-10-Data.db");
        
		System.setProperty("cassandra.config", CASSANDRA_YAML);		
		System.setProperty("log4j.configuration", LOG4J_PROPERTIES);
        
        DatabaseDescriptor.loadSchemas();
        if (Schema.instance.getNonSystemTables().size() < 1)
        {
            String msg = "no non-system tables are defined";
            System.err.println(msg);
            throw new ConfigurationException(msg);
        }   
        
		File output = new File("/home/felipe/cassandra/outputjson.json");
		
		PrintStream ps = new PrintStream(output);
		
        export(ssTables, "/testfile");

        System.exit(0);
    }    
}