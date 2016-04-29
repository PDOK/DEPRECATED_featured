package pdok.featured;

import com.ctc.wstx.sax.WstxSAXParserFactory;
import com.ctc.wstx.stax.WstxInputFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import org.geotools.xml.Configuration;
import org.geotools.xml.Parser;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

/**
 *
 * @author kroonr
 */
public class WstxParser extends Parser {

    private final WstxSAXParserFactory factory;

    public WstxParser(Configuration config) throws SAXNotRecognizedException, SAXNotSupportedException {
        super(config);
        WstxInputFactory inputFactory = new WstxInputFactory();
        inputFactory.configureForSpeed();

        factory = new WstxSAXParserFactory(inputFactory);

    }

    /**
     * Woodstox parser;
     * @param validate
     * @return
     * @throws ParserConfigurationException
     * @throws SAXException
     */
    @Override
    protected SAXParser parser(boolean validate) throws ParserConfigurationException, SAXException {
        return factory.newSAXParser();
    }
}
