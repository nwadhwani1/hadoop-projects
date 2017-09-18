package p1;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AVRODriver {
	
	public static final String INPUT_DIR = "hdfs://localhost:8020/user/cloudera/AVRO";
	public static final String OUTPUT_DIR="hdfs://localhost:8020/user/cloudera/output_2";

	public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		
		Path input_Path = new Path(INPUT_DIR);
		Path ouput_Path = new Path(OUTPUT_DIR);
		Configuration conf = new Configuration();
		try {
			Job job = Job.getInstance(conf, "AVRO JOB");
			job.setJarByClass(AVRODriver.class);
			job.setMapperClass(AVROMapper.class);
			job.setReducerClass(AVROReducer.class);
			
			//job.setOutputKeyClass(Text.class);
			//job.setOutputValueClass(NullWritable.class);
			
			job.setInputFormatClass(AvroKeyInputFormat.class);
			job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
			
			AvroJob.setInputKeySchema(job, Student.getClassSchema());
			AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
			AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.FLOAT));

			FileInputFormat.addInputPath(job, input_Path);
			FileOutputFormat.setOutputPath(job, ouput_Path);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			
		
			ouput_Path.getFileSystem(conf).delete(ouput_Path, true);
			
			System.exit(job.waitForCompletion(true) ? 0:1);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		

	}

}
