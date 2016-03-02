package org.mapdb.serializer;

import org.mapdb.DBUtil;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/29/16.
 */
public abstract class GroupSerializerObjectArray<A> implements GroupSerializer<A,Object[]> {


    @Override public void valueArraySerialize(DataOutput2 out, Object[] vals) throws IOException {
        for(Object o:vals){
            serialize(out, (A) o);
        }
    }

    @Override public Object[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        Object[] ret = new Object[size];
        for(int i=0;i<size;i++){
            ret[i] = deserialize(in,-1);
        }
        return ret;
    }

    @Override public A valueArrayGet(Object[] vals, int pos){
        return (A) vals[pos];
    }

    @Override public int valueArraySize(Object[] vals){
        return vals.length;
    }

    @Override public Object[] valueArrayEmpty(){
        return new Object[0];
    }

    @Override public Object[] valueArrayPut(Object[] vals, int pos, A newValue) {
        return DBUtil.arrayPut(vals, pos, newValue);
    }

    @Override public Object[] valueArrayUpdateVal(Object[] vals, int pos, A newValue) {
        vals = vals.clone();
        vals[pos] = newValue;
        return vals;
    }

    @Override public Object[] valueArrayFromArray(Object[] objects) {
        return objects;
    }

    @Override public Object[] valueArrayCopyOfRange(Object[] vals, int from, int to) {
        return Arrays.copyOfRange(vals, from, to);
    }

    @Override public Object[] valueArrayDeleteValue(Object[] vals, int pos) {
        return DBUtil.arrayDelete(vals, pos, 1);
    }

    @Override public Object[] valueArrayToArray(Object[] vals){
        Object[] ret = new Object[valueArraySize(vals)];
        for(int i=0;i<ret.length;i++){
            ret[i] = valueArrayGet(vals,i);
        }
        return ret;
    }

    @Override public  int valueArraySearch(Object[] keys, A key){
        return Arrays.binarySearch(keys, key, (Comparator<Object>)this);
    }

    @Override public  int valueArraySearch(Object[] keys, A key, Comparator comparator){
        if(comparator==this)
            return valueArraySearch(keys, key);
        return Arrays.binarySearch(keys, key, comparator);
    }


}
