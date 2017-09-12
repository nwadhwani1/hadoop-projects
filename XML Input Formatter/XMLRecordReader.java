package p1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class XMLRecordReader extends RecordReader<LongWritable, Text> {

	public static final String Start_tag_key = "xmlinput.start";
	public static final String End_tag_key = "xmlinput.end";
	public static final String utf8 = "utf-8";
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	private FSDataInputStream fsin;
	private long start, end;
	private byte[] startTag, endTag;
	private DataOutputBuffer buffer = new DataOutputBuffer();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		System.out.println("XMLRecord Reader Initialize");
		Configuration conf = context.getConfiguration();
		startTag = conf.get(Start_tag_key).getBytes(utf8);
		endTag = conf.get(End_tag_key).getBytes(utf8);
		FileSplit fileSplit = (FileSplit) split;
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
		System.out.println("START POS:" + start + " END POS: " + end);

		// get access to file
		Path file = fileSplit.getPath();
		System.out.println("path is :" + file);

		// get access to HDFS
		FileSystem fs = file.getFileSystem(conf);
		fsin = fs.open(file);
		fsin.seek(start);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		System.out.println("XML Input Format next key value");
		if (fsin.getPos()<end)
		{
			if(readUntilMatch(startTag, false))
			{
				try
				{
				buffer.write(startTag);
				
				if(readUntilMatch(endTag,true))
				{
					key.set(fsin.getPos());
					value.set(buffer.getData(),0,buffer.getLength());
					return true;
				}
				}
				finally
				{
					buffer.reset();
				}
			}
			
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return fsin.getPos()/(float)end;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		fsin.close();

	}

	private boolean readUntilMatch(byte[] startTag, boolean withinBlock)
			throws IOException {
		int i = 0;
		while (true) {
			int b = fsin.read();

			if (b == -1)
				return false;
			if (withinBlock)
				buffer.write(b);

			if (b == startTag[i]) {
				i++;
				if (i >= startTag.length)// are all characters matched
				{
					return true;
				}
			} else {
				i = 0;// if any character did not match,reset i to 0
			}
			// see if we are trying to read the <employee> tag but we have cross
			// the end point.
			if (!withinBlock && i == 0 && fsin.getPos() >= end)
				return false;
		}

	}

}
