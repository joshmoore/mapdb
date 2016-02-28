package org.mapdb.serializer;

import org.mapdb.DataIO;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerRecidArray implements Serializer<long[]> {
    @Override
    public void serialize(DataOutput2 out, long[] value) throws IOException {
        out.packInt(value.length);
        for (long recid : value) {
            DataIO.packRecid(out, recid);
        }
    }

    @Override
    public long[] deserialize(DataInput2 in, int available) throws IOException {
        int size = in.unpackInt();
        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = DataIO.unpackRecid(in);
        }
        return ret;
    }

    ;


    @Override
    public int valueArraySearch(Object keys, long[] key) {
        return LONG_ARRAY.valueArraySearch(keys, key);
    }

    @Override
    public int valueArraySearch(Object keys, long[] key, Comparator comparator) {
        return LONG_ARRAY.valueArraySearch(keys, key, comparator);
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(long[] a1, long[] a2) {
        return Arrays.equals(a1, a2);
    }

    @Override
    public int hashCode(long[] bytes, int seed) {
        return LONG_ARRAY.hashCode(bytes, seed);
    }

    @Override
    public int compare(long[] o1, long[] o2) {
        return LONG_ARRAY.compare(o1, o2);
    }

}
