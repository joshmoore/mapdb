package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerLong extends LongSerializer {

    @Override
    public void serialize(DataOutput2 out, Long value) throws IOException {
        out.writeLong(value);
    }

    @Override
    public Long deserialize(DataInput2 in, int available) throws IOException {
        return new Long(in.readLong());
    }

}
