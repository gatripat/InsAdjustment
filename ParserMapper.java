package com.barclays.ifrs9;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 *
 * @author gatripat
 */
public class ParserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	@Override
	public void map(LongWritable key, Text value1, Context context)

	throws IOException, InterruptedException {

		String xmlString = value1.toString();

		SAXBuilder builder = new SAXBuilder();
		Reader in = new StringReader(xmlString);
		String value = "";
		try {

			Document doc = builder.build(in);
			Element root = doc.getRootElement();

			String tag1 = root.getChild("<haver:DataSet>").getChild("<haver:Series>").getTextTrim();
			String tag2 = root.getChild("<haver:DataSet>").getChild("<haver:Series>").getChild("<haver:Obs>").getTextTrim();
			
			Logger.getLogger(ParserMapper.class.getName()).log(Level.INFO, null, "tag1>> "+tag1);
			Logger.getLogger(ParserMapper.class.getName()).log(Level.INFO, null, "tag2 >> "+tag2);
			context.write(NullWritable.get(), new Text(tag2));
		} catch (JDOMException ex) {
			Logger.getLogger(ParserMapper.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(ParserMapper.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

}
