package doc;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.ArraySer;


public class btreemap_object_array {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        BTreeMap<Object[], Long> map = db.treeMap("map")
                // use array serializer for unknown objects
                // TODO db.getDefaultSerializer()
                .keySerializer(new ArraySer(Serializer.JAVA))
                // or use serializer for specific objects such as String
                .keySerializer(new ArraySer(Serializer.STRING))
                .createOrOpen();
        //z
    }
}
