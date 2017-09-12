package p1;

import java.io.IOException;
import java.io.StringBufferInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class XMLMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public XMLMapper() {
		System.out.println("Mapper");
	}
	
	
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String document = value.toString();
		System.out.println("document : " + document);
		
		try {
		DocumentBuilderFactory documentBuilder = DocumentBuilderFactory.newInstance();
		
			DocumentBuilder builder = documentBuilder.newDocumentBuilder();
			@SuppressWarnings("deprecation")
			Document doc = builder.parse(new StringBufferInputStream(document));
			doc.getDocumentElement().normalize();
			System.out.println("root element : " + doc.getDocumentElement().getNodeName());
			String empId = doc.getElementsByTagName("empId").item(0).getTextContent();
			String empName=doc.getElementsByTagName("empName").item(0).getTextContent();
			String empSalary=doc.getElementsByTagName("empSalary").item(0).getTextContent();
			String empEmail=doc.getElementsByTagName("empEmail").item(0).getTextContent();
		   
		    System.out.println(empId);
		    System.out.println(empName);
		    System.out.println(empSalary);
		    System.out.println(empEmail);
		    
		    context.write(new Text("1"),new Text(empId+" "+empName+" "+empSalary+" "+empEmail));
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
