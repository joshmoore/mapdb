package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerDate extends EightByteSerializer<Date> {

    @Override
    public void serialize(DataOutput2 out, Date value) throws IOException {
        out.writeLong(value.getTime());
    }

    @Override
    public Date deserialize(DataInput2 in, int available) throws IOException {
        return new Date(in.readLong());
    }

    @Override
    protected Date unpack(long l) {
        return new Date(l);
    }

    @Override
    protected long pack(Date l) {
        return l.getTime();
    }

    @Override
    final public int valueArraySearch(Object keys, Date key) {
        long time = ((Date) key).getTime();
        return Arrays.binarySearch((long[]) keys, time);
    }


}
