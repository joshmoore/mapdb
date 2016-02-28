package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerInteger2 extends SerializerInteger {

    @Override
    public void serialize(DataOutput2 out, Integer value) throws IOException {
        out.writeInt(value);
    }

    @Override
    public Integer deserialize(DataInput2 in, int available) throws IOException {
        return new Integer(in.readInt());
    }

}
