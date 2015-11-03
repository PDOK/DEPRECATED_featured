package pdok.featured.converters;

import com.vividsolutions.jts.geom.CoordinateSequenceFactory;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class Transformer {
    
    public final static Transformer RDToETRS89 = new Transformer(4258, new RDToETRS89SequenceFactory());
    public final static Transformer ETRS89ToRD = new Transformer(28992, new ETRS89ToRDSequenceFactory());
    
    private final int SRID;
    private final GeometryFactory factory;

    public Transformer(int SRID, CoordinateSequenceFactory sequenceFactory) {
        this.SRID = SRID;
        factory = new GeometryFactory(new PrecisionModel(), SRID, sequenceFactory);
    }
    
            
    public Geometry transform (Geometry geom) {
        Geometry transformed = factory.createGeometry(geom);
        transformed.setSRID(SRID);
        
        return transformed;
    }
    
}