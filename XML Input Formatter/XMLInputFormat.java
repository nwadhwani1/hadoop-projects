package p1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class XMLInputFormat extends TextInputFormat{
	
	public XMLInputFormat() {
		System.out.println("Input Format Constructor");
	}
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		System.out.println("Record Reader");
		return new XMLRecordReader ();
	}
	
}