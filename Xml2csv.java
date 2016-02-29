import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Document;

class Xml2csv {

    public static void main(String args[]) throws Exception {
        File stylesheet1 = new File("C:\\Users\\TEMP\\IdeaProjects\\ukdb\\src\\uk_stylesheet.xsl");

        File xmlSource = new File("C:\\Users\\TEMP\\IdeaProjects\\ukdb\\src\\UK_CMPLT.xml");

        DocumentBuilderFactory factory1 = DocumentBuilderFactory.newInstance();

        DocumentBuilder builder1 = factory1.newDocumentBuilder();

        Document document1 = builder1.parse(xmlSource);

        StreamSource stylesource1 = new StreamSource(stylesheet1);

        Transformer transformer1 = TransformerFactory.newInstance()
                .newTransformer(stylesource1);


        Source source1 = new DOMSource(document1);


        Result outputTarget1 = new StreamResult(new File("C:\\Users\\TEMP\\IdeaProjects\\ukdb\\src\\output.csv"));

        transformer1.transform(source1, outputTarget1);

    }
}