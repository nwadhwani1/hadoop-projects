package p1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class pdfDriver {

	public static final String INPUT_DIR = "hdfs://localhost:8020/user/cloudera/Input_Path";
	public static final String OUTPUT_DIR = "hdfs://localhost:8020/user/cloudera/PDFtoTEXT";
	public static boolean flag;

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Path input_Path = new Path(INPUT_DIR);
		Path output_Path = new Path(OUTPUT_DIR);
		pdfIOUtils.uploadInputFile(INPUT_DIR);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PDF job");

		job.setJarByClass(pdfDriver.class);
		job.setMapperClass(pdfMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(pdfInputFormat.class);
		FileInputFormat.addInputPath(job, input_Path);
		FileOutputFormat.setOutputPath(job, output_Path);

		output_Path.getFileSystem(conf).delete(output_Path, true);

		flag = job.waitForCompletion(true);
		if (flag)
			pdfIOUtils.readOutputFile(OUTPUT_DIR);

	}

}
