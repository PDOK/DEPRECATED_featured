package pdok.featured.converters;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFactory;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory;
import java.io.IOException;
import static rdnaptrans.Transform.rdnap2etrs;
import rdnaptrans.value.Geographic;
import rdnaptrans.value.Cartesian;

public class RDToETRS89SequenceFactory implements CoordinateSequenceFactory {

    public CoordinateSequence create(Coordinate[] coordinates) {

        Coordinate[] result = new Coordinate[coordinates.length];

        for (int i = 0; i < coordinates.length; i++) {
            boolean withZ = !Double.isNaN(coordinates[i].z);
            Cartesian rd = Helpers.toCartesian(coordinates[i], withZ);
            try {
                Geographic etrs = rdnap2etrs(rd);
                result[i] = Helpers.toCoordinate(etrs, withZ);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        return CoordinateArraySequenceFactory.instance().create(result);
    }

    public CoordinateSequence create(CoordinateSequence coordSeq) {
        return create(coordSeq.toCoordinateArray());
    }

    public CoordinateSequence create(int size, int dimension) {
        if (dimension > 3) {
            throw new IllegalArgumentException("dimension must be <= 3");
        }

        return new CoordinateArraySequence(size, dimension);
    }
}