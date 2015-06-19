package pdok.featured.xslt;

import java.io.IOException;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.InputStream;

import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

public class TransformXSLT {
        
       private Templates templates;
        
        public TransformXSLT(InputStream xsltStream) throws TransformerConfigurationException {
                TransformerFactory factory = TransformerFactory.newInstance("org.apache.xalan.xsltc.trax.TransformerFactoryImpl", null);
                templates = factory.newTemplates(new StreamSource(xsltStream));

        }
        
        public String transform(String input)
                        throws TransformerException, IOException {
            Transformer transformer = templates.newTransformer();
            StringWriter writer = new StringWriter();
            transformer.transform(new StreamSource(new StringReader(input) ), new StreamResult(writer));
            return writer.getBuffer().toString();
        }
    }
    

