package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerStringNoSize extends StringSerializer {

    private final Charset UTF8_CHARSET = Charset.forName("UTF8");

    @Override
    public void serialize(DataOutput2 out, String value) throws IOException {
        final byte[] bytes = value.getBytes(UTF8_CHARSET);
        out.write(bytes);
    }


    @Override
    public String deserialize(DataInput2 in, int available) throws IOException {
        if (available == -1) throw new IllegalArgumentException("STRING_NOSIZE does not work with collections.");
        byte[] bytes = new byte[available];
        in.readFully(bytes);
        return new String(bytes, UTF8_CHARSET);
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean needsAvailableSizeHint() {
        return true;
    }

    @Override
    public Object valueArrayCopyOfRange(Object vals, int from, int to) {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public Object valueArrayDeleteValue(Object vals, int pos) {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public Object valueArrayDeserialize(DataInput2 in2, int size) throws IOException {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public Object valueArrayEmpty() {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public Object valueArrayFromArray(Object[] objects) {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public String valueArrayGet(Object vals, int pos) {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public Object valueArrayPut(Object vals, int pos, String newValue) {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public int valueArraySearch(Object keys, String key) {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public int valueArraySearch(Object keys, String key, Comparator comparator) {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public void valueArraySerialize(DataOutput2 out2, Object vals) throws IOException {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public int valueArraySize(Object vals) {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }

    @Override
    public Object valueArrayUpdateVal(Object vals, int pos, String newValue) {
        throw new UnsupportedOperationException("NOSIZE can not be used for values");
    }
}
