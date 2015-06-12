package pdok.featured.tiles;

/**
 *
 * @author Raymond Kroon <raymond@k3n.nl>
 */

public class NLTile {
    public final static int RD_LOWER_X = -100000;
    public final static int RD_UPPER_X = 412000;
    public final static int RD_LOWER_Y = 200000;
    public final static int RD_UPPER_Y = 712000;
    
    public static int getTileFromRD(double x, double y) {
        if (x >= RD_UPPER_X || x < RD_LOWER_X ) {
            return -1;
        }
        
        if (y >= RD_UPPER_Y || y < RD_LOWER_Y) {
            return -1;
        }
        
        byte i_x = (byte)((x - RD_LOWER_X) / 2000);
        byte i_y = (byte)((y - RD_LOWER_Y) / 2000);
        
        return ZOrder.for2D(i_x, i_y);
    }
 }
