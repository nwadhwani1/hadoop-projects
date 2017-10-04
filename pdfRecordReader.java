package p1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

public class pdfRecordReader extends RecordReader<LongWritable, Text> {
	
	private LongWritable key=null;
	private Text value=null;
	private String[] lines=null;

	
	public pdfRecordReader() {
		System.out.println("PDF Record Reader");
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		FileSplit fileSplit = (FileSplit)split;
		Configuration conf = context.getConfiguration();
		Path path = fileSplit.getPath();
		
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream fin = fs.open(fileSplit.getPath());
		PDDocument pdfDocument=null;
		PDFTextStripper textStripper=new PDFTextStripper();
		String parsedLine = null;
		pdfDocument = PDDocument.load(fin);
		parsedLine = textStripper.getText(pdfDocument);
		lines = parsedLine.split("\n");
		
		
 		
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if(key==null)
		{
			key = new LongWritable(1);
			value=new Text(lines[0]);
			
		}
		
		else
		{
			int temp = (int) key.get();
			if(temp <=lines.length-1)
			{
				int count = (int)key.get();
				value = new Text(lines[count]);
				count+=1;
				key = new LongWritable(count);
				
			}
			else
			{
				return false;
			}
			
			
		
		}
		
		if(key==null || value==null)
			return false;
		else
			return true;
		
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
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
