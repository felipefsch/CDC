package logBased;

import java.io.IOException;

//import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

@SuppressWarnings("deprecation")
public class MutationRecordReader implements RecordReader<NullWritable, BytesWritable>{

	private long start;
	private long pos;
	private long end;
	private NullWritable key = NullWritable.get();
	private FSDataInputStream in;
	//private FastByteArrayInputStream bufIn;
	  
	public MutationRecordReader(JobConf conf, FileSplit split) throws Exception {
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();		  
		  
		FileSystem fs = file.getFileSystem(conf);
		in = fs.open(split.getPath());
		  
		pos = in.getPos();
	}
	  
	@Override
	public NullWritable createKey() {
		return key;
	}
	  
	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
	}
	  

	@Override
	public void close() throws IOException {
	    if (in != null) {
	        in.close(); 
	      }		
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public float getProgress() throws IOException {
	    if (start == end) {
	        return 0.0f;
	    } else {
	    	return Math.min(1.0f, (pos - start) / (float)(end - start));
	    }
	}

	// Read a Mutation Row
	@Override
	@SuppressWarnings("unused")
	public synchronized boolean next(NullWritable key, BytesWritable value)
			throws IOException {
		byte[] bytes = new byte[4096];
		
		key = this.key;
		
		if (pos < end) {
			try {				
				int serializedSize = in.readInt();
				long checksum = in.readLong();
				in.readFully(bytes, 0, serializedSize);
				checksum = in.readLong();
				pos = in.getPos();
				
				value.set(bytes, 0, serializedSize);
			}
			catch (Exception e) {
				return false;
			}
		}
		else {
			return false;
		}		
		return true;		
	}
	  
}
