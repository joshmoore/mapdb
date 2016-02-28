package org.mapdb.serializer;

import org.mapdb.DataIO;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerByteArray implements Serializer<byte[]> {

    @Override
    public void serialize(DataOutput2 out, byte[] value) throws IOException {
        out.packInt(value.length);
        out.write(value);
    }

    @Override
    public byte[] deserialize(DataInput2 in, int available) throws IOException {
        int size = in.unpackInt();
        byte[] ret = new byte[size];
        in.readFully(ret);
        return ret;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(byte[] a1, byte[] a2) {
        return Arrays.equals(a1, a2);
    }

    public int hashCode(byte[] bytes, int seed) {
        return DataIO.longHash(
                DataIO.hash(bytes, 0, bytes.length, seed));
    }

    @Override
    public int compare(byte[] o1, byte[] o2) {
        if (o1 == o2) return 0;
        final int len = Math.min(o1.length, o2.length);
        for (int i = 0; i < len; i++) {
            int b1 = o1[i] & 0xFF;
            int b2 = o2[i] & 0xFF;
            if (b1 != b2)
                return b1 - b2;
        }
        return o1.length - o2.length;
    }
}
