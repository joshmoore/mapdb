package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerBoolean implements GroupSerializer<Boolean,boolean[]> {

    @Override
    public void serialize(DataOutput2 out, Boolean value) throws IOException {
        out.writeBoolean(value);
    }

    @Override
    public Boolean deserialize(DataInput2 in, int available) throws IOException {
        return in.readBoolean();
    }

    @Override
    public int fixedSize() {
        return 1;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public int valueArraySearch(boolean[] keys, Boolean key) {
        return Arrays.binarySearch(valueArrayToArray(keys), key);
    }

    @Override
    public int valueArraySearch(boolean[] keys, Boolean key, Comparator comparator) {
        return Arrays.binarySearch(valueArrayToArray(keys), key, comparator);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, boolean[] vals) throws IOException {
        for (boolean b : vals) {
            out.writeBoolean(b);
        }
    }

    @Override
    public boolean[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        boolean[] ret = new boolean[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readBoolean();
        }
        return ret;
    }

    @Override
    public Boolean valueArrayGet(boolean[] vals, int pos) {
        return vals[pos];
    }

    @Override
    public int valueArraySize(boolean[] vals) {
        return vals.length;
    }

    @Override
    public boolean[] valueArrayEmpty() {
        return new boolean[0];
    }

    @Override
    public boolean[] valueArrayPut(boolean[] array, int pos, Boolean newValue) {
        final boolean[] ret = Arrays.copyOf(array, array.length + 1);
        if (pos < array.length) {
            System.arraycopy(array, pos, ret, pos + 1, array.length - pos);
        }
        ret[pos] = newValue;
        return ret;

    }

    @Override
    public boolean[] valueArrayUpdateVal(boolean[] vals, int pos, Boolean newValue) {
        vals = vals.clone();
        vals[pos] = newValue;
        return vals;

    }

    @Override
    public boolean[] valueArrayFromArray(Object[] objects) {
        boolean[] ret = new boolean[objects.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = (Boolean) objects[i];
        }
        return ret;
    }

    @Override
    public boolean[] valueArrayCopyOfRange(boolean[] vals, int from, int to) {
        return Arrays.copyOfRange(vals, from, to);
    }

    @Override
    public boolean[] valueArrayDeleteValue(boolean[] vals, int pos) {
        boolean[] vals2 = new boolean[vals.length - 1];
        System.arraycopy(vals, 0, vals2, 0, pos - 1);
        System.arraycopy(vals, pos, vals2, pos - 1, vals2.length - (pos - 1));
        return vals2;

    }
}
