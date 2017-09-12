package p1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class XMLReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,
			Reducer<Text, Text, Text, Text>.Context arg2) throws IOException,
			InterruptedException {
		Integer max_salary=Integer.MIN_VALUE;
		Text highPaidEmployee=new Text();
		
		for(Text empDetail:arg1)
		{
			int salary=Integer.parseInt(empDetail.toString().split(" ")[2]);
			if(salary>max_salary){
				max_salary=salary;
				highPaidEmployee.set(empDetail);
			}
		}
		arg2.write(highPaidEmployee, new Text());
	}

}
