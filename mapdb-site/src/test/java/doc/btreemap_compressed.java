package doc;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.CompressionWrapper;


public class btreemap_compressed {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        BTreeMap<Long, String> map = db.treeMap("map")
                //TODO external values are not supported yet
                //.valuesOutsideNodesEnable()
                .valueSerializer(new CompressionWrapper(Serializer.STRING))
                .createOrOpen();
        //z
    }
}
