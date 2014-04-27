package logBased;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


@SuppressWarnings("deprecation")
public class SegmentInputFormat extends FileInputFormat<NullWritable, BytesWritable>{
	
//	@Override
//	protected boolean isSplitable(FileSystem fs, Path filename) {
//	   return false;
//	}
	
	@Override
	public RecordReader<NullWritable, BytesWritable> getRecordReader(
			InputSplit split, JobConf job,
			Reporter reporter) throws IOException {

		reporter.setStatus(split.toString());		
		try {
			return new MutationRecordReader(job, (FileSplit) split);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}


	
}
