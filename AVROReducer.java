package p1;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AVROReducer extends Reducer<IntWritable, Text, AvroKey<Integer>, AvroKey<Float>> {
	float total =0f;
	@Override
	protected void reduce(
			IntWritable arg0,
			Iterable<Text> arg1,
			Reducer<IntWritable, Text, AvroKey<Integer>, AvroKey<Float>>.Context arg2)
			throws IOException, InterruptedException {
		
		arg1.forEach(value -> {
			System.out.println(value);
			total+=  Float.parseFloat(value.toString().split("-")[1]);
		});
		
		arg2.write(new AvroKey<Integer>(arg0.get()), new AvroKey<Float>(total));
		
		
	}

}
