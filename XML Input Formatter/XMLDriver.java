package p1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class XMLDriver {
	
	private static final String INPUT_DIR = "hdfs://localhost:8020/user/cloudera/XML";
	private static final String OUTPUT_DIR="hdfs://localhost:8020/user/cloudera/output_3";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Path input_Path = new Path(INPUT_DIR);
		Path output_Path = new Path(OUTPUT_DIR);
		Configuration conf = new Configuration();
		
		conf.set("xmlinput.start","<employee>");
		conf.set("xmlinput.end", "</employee>");
		
		Job job = Job.getInstance(conf, "XML_Input_Job");
		job.setJarByClass(XMLDriver.class);
		job.setMapperClass(XMLMapper.class);
		job.setReducerClass(XMLReducer.class);
		job.setInputFormatClass(XMLInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//job.setOutputFormatClass(Text.class);
		
		FileInputFormat.addInputPath(job, input_Path);
		FileOutputFormat.setOutputPath(job, output_Path);
		
		output_Path.getFileSystem(job.getConfiguration()).delete(output_Path, true);
		
		job.waitForCompletion(true);
		
		

	}

}
