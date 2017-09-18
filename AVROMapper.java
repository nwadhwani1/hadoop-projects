package p1;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AVROMapper extends Mapper<AvroKey<Student>, NullWritable, IntWritable, Text> {

	public AVROMapper() {
		System.out.println("AVRO MAPPER CONSTRUCTOR");
	}
	@Override
	protected void map(
			AvroKey<Student> key,
			NullWritable value,
			Context context)
			throws IOException, InterruptedException {
		
		Student st = key.datum();
		
		IntWritable studentID = new IntWritable(st.getStudentId());
		Text studentNAME = new Text( st.getSubjectName().toString());
		FloatWritable studentMARKS = new FloatWritable(st.getStudentMarks());
		
		context.write(studentID, new Text(studentNAME + "-" + studentMARKS));
		
		
		

	}
}
