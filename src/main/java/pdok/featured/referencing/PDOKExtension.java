package pdok.featured.referencing;

import java.net.URL;
import org.geotools.factory.Hints;
import org.geotools.referencing.factory.epsg.FactoryUsingWKT;

/**
 *
 * @author kroonr
 */
public class PDOKExtension extends FactoryUsingWKT {

    public static final String FILENAME = "pdok.properties";

    public PDOKExtension() {
        this(null);
    }

    public PDOKExtension(final Hints hints) {
        super(hints, DEFAULT_PRIORITY);
    }

    @Override
    protected URL getDefinitionsURL() {
        return PDOKExtension.class.getResource(FILENAME);
    }
}
