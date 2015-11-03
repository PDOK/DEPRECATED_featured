package pdok.featured.converters;

import rdnaptrans.value.Geographic;
import rdnaptrans.value.Cartesian;
import com.vividsolutions.jts.geom.Coordinate;

/**
 *
 * @author raymond
 */
public class Helpers {
    public static Geographic toGeographic (Coordinate coord, boolean withZ) {
        if (withZ) {
            return new Geographic(coord.x, coord.y, coord.z);
        }
        else {
            return new Geographic(coord.x, coord.y);
        }
    }
    
    public static Cartesian toCartesian (Coordinate coord, boolean withZ) {
        if (withZ) {
            return new Cartesian (coord.x, coord.y, coord.z);
        }
        else {
            return new Cartesian (coord.x, coord.y);
        }
    }
    
    public static Coordinate toCoordinate(Geographic coord, boolean withZ) {
        if (withZ) {
            return new Coordinate(coord.phi, coord.lambda, coord.h);
        }
        else {
            return new Coordinate(coord.phi, coord.lambda);
        }
    }
    
    public static Coordinate toCoordinate(Cartesian coord, boolean withZ) {
        if (withZ) {
            return new Coordinate(coord.X, coord.Y, coord.Z);
        }
        else {
            return new Coordinate(coord.X, coord.Y);
        }
    }
}
