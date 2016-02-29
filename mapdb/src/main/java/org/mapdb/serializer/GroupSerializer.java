package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by jan on 2/29/16.
 */
public interface GroupSerializer<A,E> extends Serializer<A> {

    default A valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) throws IOException {
        E keys = valueArrayDeserialize(input, keysLen);
        return valueArrayGet(keys, pos);
//        A a=null;
//        while(pos-- >= 0){
//            a = deserialize(input, -1);
//        }
//        return a;
    }



    default int valueArrayBinarySearch(A key, DataInput2 input, int keysLen, Comparator comparator) throws IOException {
        E keys = valueArrayDeserialize(input, keysLen);
        return valueArraySearch(keys, key, comparator);
//        for(int pos=0; pos<keysLen; pos++){
//            A from = deserialize(input, -1);
//            int comp = compare(key, from);
//            if(comp==0)
//                return pos;
//            if(comp<0)
//                return -(pos+1);
//        }
//        return -(keysLen+1);
    }


    int valueArraySearch(E keys, A key);

    int valueArraySearch(E keys, A key, Comparator comparator);

    void valueArraySerialize(DataOutput2 out, E vals) throws IOException;

    E valueArrayDeserialize(DataInput2 in, int size) throws IOException;

    A valueArrayGet(E vals, int pos);

    int valueArraySize(E vals);

    E valueArrayEmpty();

    E valueArrayPut(E vals, int pos, A newValue);


    E valueArrayUpdateVal(E vals, int pos, A newValue);

    E valueArrayFromArray(Object[] objects);

    E valueArrayCopyOfRange(E vals, int from, int to);

    E valueArrayDeleteValue(E vals, int pos);

    default Object[] valueArrayToArray(E vals){
        Object[] ret = new Object[valueArraySize(vals)];
        for(int i=0;i<ret.length;i++){
            ret[i] = valueArrayGet(vals,i);
        }
        return ret;
    }

}
