package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class StringSerializer implements Serializer<String> {

    @Override
    public void serialize(DataOutput2 out, String value) throws IOException {
        out.writeUTF(value);
    }

    @Override
    public String deserialize(DataInput2 in, int available) throws IOException {
        return in.readUTF();
    }

    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public void valueArraySerialize(DataOutput2 out2, Object vals) throws IOException {
        char[][] vals2 = (char[][]) vals;
        for(char[] v:vals2){
            out2.packInt(v.length);
            for(char c:v){
                out2.packInt(c);
            }
        }
    }

    @Override
    public Object valueArrayDeserialize(DataInput2 in2, int size) throws IOException {
        char[][] ret = new char[size][];
        for(int i=0;i<size;i++){
            int size2 = in2.unpackInt();
            char[] cc = new char[size2];
            for(int j=0;j<size2;j++){
                cc[j] = (char) in2.unpackInt();
            }
            ret[i] = cc;
        }
        return ret;
    }

    @Override
    public int valueArraySearch(Object keys, String key) {
        char[] key2 = key.toCharArray();
        return Arrays.binarySearch((char[][])keys, key2, CHAR_ARRAY);
    }

    @Override
    public int valueArraySearch(Object keys, String key, Comparator comparator) {
        char[][] array = (char[][]) keys;

        int lo = 0;
        int hi = array.length - 1;

        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int compare = comparator.compare(key, new String(array[mid]));

            if (compare == 0)
                return mid;
            else if (compare < 0)
                hi = mid - 1;
            else
                lo = mid + 1;
        }
        return -(lo + 1);
    }

    @Override
    public String valueArrayGet(Object vals, int pos) {
        return new String(((char[][])vals)[pos]);
    }

    @Override
    public int valueArraySize(Object vals) {
        return ((char[][])vals).length;
    }

    @Override
    public Object valueArrayEmpty() {
        return new char[0][];
    }

    @Override
    public Object valueArrayPut(Object vals, int pos, String newValue) {
        char[][] array = (char[][]) vals;
        final char[][] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = newValue.toCharArray();
        return ret;

    }

    @Override
    public Object valueArrayUpdateVal(Object vals, int pos, String newValue) {
        char[][] cc = (char[][]) vals;
        cc = cc.clone();
        cc[pos] = newValue.toCharArray();
        return cc;
    }

    @Override
    public Object valueArrayFromArray(Object[] objects) {
        char[][] ret = new char[objects.length][];
        for(int i=0;i<ret.length;i++){
            ret[i] = ((String)objects[i]).toCharArray();
        }
        return ret;
    }

    @Override
    public Object valueArrayCopyOfRange(Object vals, int from, int to) {
        return Arrays.copyOfRange((char[][]) vals, from, to);
    }

    @Override
    public Object valueArrayDeleteValue(Object vals, int pos) {
        char[][] valsOrig = (char[][]) vals;
        char[][] vals2 = new char[valsOrig.length-1][];
        System.arraycopy(vals,0,vals2, 0, pos-1);
        System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
        return vals2;
    }


    @Override
    public int hashCode(String s, int seed) {
        char[] c = s.toCharArray();
        return CHAR_ARRAY.hashCode(c, seed);
    }

}
