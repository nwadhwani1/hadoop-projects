package p1;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class pdfIOUtils {

	public static void uploadInputFile(String inputDIR) throws IOException {
		Path inputPath = new Path(inputDIR);
		String localPath = "src/main/resources";
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(URI.create(inputDIR), conf);
		fs.delete(inputPath, true);

		File file = new File(localPath);
		File[] listOfFiles = file.listFiles();

		for (File list : listOfFiles) {
			InputStream in = new BufferedInputStream(new FileInputStream(
					localPath + "/" + list.getName()));
			OutputStream op = fs.create(new Path(inputDIR + "/"
					+ list.getName()));
			IOUtils.copyBytes(in, op, 4096, true);

		}

	}

	public static void readOutputFile(String outputDIR) throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(outputDIR + "/"+ "part-m-00000"), conf);
		FSDataInputStream fin = fs.open(new Path(outputDIR + "/"+ "part-m-00000"));
		IOUtils.copyBytes(fin, System.out, 4096, false);
		
		

	}

}
