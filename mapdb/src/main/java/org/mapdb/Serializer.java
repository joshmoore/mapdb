/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.mapdb;


import org.jetbrains.annotations.NotNull;
import org.mapdb.serializer.*;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Provides serialization and deserialization
 *
 * @author Jan Kotek
 */
public interface Serializer<A> extends Comparator<A> {


    Serializer<Character> CHAR = new SerializerChar();



    /**
     * <p>
     * Serializes strings using UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     * </p><p>
     * Unlike {@link Serializer#STRING} this method hashes String with {@link String#hashCode()}
     * </p>
     */
    Serializer<String> STRING_ORIGHASH = new SerializerStringOrigHash();

    /**
     * Serializes strings using UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    Serializer<String> STRING = new StringSerializer2();




    /**
     * Serializes strings using UTF8 encoding.
     * Deserialized String is interned {@link String#intern()},
     * so it could save some memory.
     *
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    Serializer<String> STRING_INTERN = new SerializerStringIntern();

    /**
     * Serializes strings using ASCII encoding (8 bit character).
     * Is faster compared to UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    Serializer<String> STRING_ASCII = new SerializerStringAscii();

    /**
     * Serializes strings using UTF8 encoding.
     * Used mainly for testing.
     * Does not handle null values.
     */
    Serializer<String> STRING_NOSIZE = new SerializerStringNoSize();





    /** Serializes Long into 8 bytes, used mainly for testing.
     * Does not handle null values.*/

    Serializer<Long> LONG = new SerializerLong();

    /**
     *  Packs positive LONG, so smaller positive values occupy less than 8 bytes.
     *  Large and negative values could occupy 8 or 9 bytes.
     */
    Serializer<Long> LONG_PACKED = new SerializerLongPacked();

    /**
     * Applies delta packing on {@code java.lang.Long}.
     * Difference between consequential numbers is also packed itself, so for small diffs it takes only single byte per
     * number.
     */
    Serializer<Long> LONG_DELTA = new SerializerLongDelta();


    /** Serializes Integer into 4 bytes, used mainly for testing.
     * Does not handle null values.*/

    Serializer<Integer> INTEGER = new SerializerInteger2();

    /**
     *  Packs positive Integer, so smaller positive values occupy less than 4 bytes.
     *  Large and negative values could occupy 4 or 5 bytes.
     */
    Serializer<Integer> INTEGER_PACKED = new SerializerIntegerPacked();


    /**
     * Applies delta packing on {@code java.lang.Integer}.
     * Difference between consequential numbers is also packed itself, so for small diffs it takes only single byte per
     * number.
     */
    Serializer<Integer> INTEGER_DELTA = new SerializerIntegerDelta();


    Serializer<Boolean> BOOLEAN = new SerializerBoolean();

    ;


    /** Packs recid + it adds 3bits checksum. */

    Serializer<Long> RECID = new SerializerRecid();

    Serializer<long[]> RECID_ARRAY = new SerializerRecidArray();

    /**
     * Always throws {@link IllegalAccessError} when invoked. Useful for testing and assertions.
     */
    Serializer<Object> ILLEGAL_ACCESS = new SerializerIllegalAccess();


    /**
     * Serializes {@code byte[]} it adds header which contains size information
     */
    Serializer<byte[] > BYTE_ARRAY = new SerializerByteArray();

    /**
     * Serializes {@code byte[]} directly into underlying store
     * It does not store size, so it can not be used in Maps and other collections.
     */
    Serializer<byte[] > BYTE_ARRAY_NOSIZE = new SerializerByteArrayNoSize();

    /**
     * Serializes {@code char[]} it adds header which contains size information
     */
    Serializer<char[] > CHAR_ARRAY = new SerializerCharArray();


    /**
     * Serializes {@code int[]} it adds header which contains size information
     */
    Serializer<int[] > INT_ARRAY = new SerializerIntArray();

    /**
     * Serializes {@code long[]} it adds header which contains size information
     */
    Serializer<long[] > LONG_ARRAY = new SerializerLongArray();

    /**
     * Serializes {@code double[]} it adds header which contains size information
     */
    Serializer<double[] > DOUBLE_ARRAY = new SerializerDoubleArray();


    /** Serializer which uses standard Java Serialization with {@link java.io.ObjectInputStream} and {@link java.io.ObjectOutputStream} */
    Serializer JAVA = new SerializerJava();

    /** Serializers {@link java.util.UUID} class */
    Serializer<java.util.UUID> UUID = new SerializerUUID();

    Serializer<Byte> BYTE = new SerializerByte();

    Serializer<Float> FLOAT = new SerializerFloat();


    Serializer<Double> DOUBLE = new SerializerDouble();

    Serializer<Short> SHORT = new SerializerShort();

// TODO boolean array
//    Serializer<boolean[]> BOOLEAN_ARRAY = new Serializer<boolean[]>() {
//        @Override
//        public void serialize(DataOutput2 out, boolean[] value) throws IOException {
//            out.packInt( value.length);//write the number of booleans not the number of bytes
//            SerializerBase.writeBooleanArray(out,value);
//        }
//
//        @Override
//        public boolean[] deserialize(DataInput2 in, int available) throws IOException {
//            int size = in.unpackInt();
//            return SerializerBase.readBooleanArray(size, in);
//        }
//
//        @Override
//        public boolean isTrusted() {
//            return true;
//        }
//
//        @Override
//        public boolean equals(boolean[] a1, boolean[] a2) {
//            return Arrays.equals(a1,a2);
//        }
//
//        @Override
//        public int hashCode(boolean[] booleans, int seed) {
//            return Arrays.hashCode(booleans);
//        }
//    };



    Serializer<short[]> SHORT_ARRAY = new SerializerShortArray();


    Serializer<float[]> FLOAT_ARRAY = new SerializerFloatArray();

    Serializer<BigInteger> BIG_INTEGER = new SerializerBigInteger();

    Serializer<BigDecimal> BIG_DECIMAL = new SerializerBigDecimal();


    Serializer<Class<?>> CLASS = new SerializerClass();

    Serializer<Date> DATE = new SerializerDate();


    //    //this has to be lazily initialized due to circular dependencies
//    static final  class __BasicInstance {
//        final static Serializer<Object> s = new SerializerBase();
//    }
//
//
//    /**
//     * Basic serializer for most classes in {@code java.lang} and {@code java.util} packages.
//     * It does not handle custom POJO classes. It also does not handle classes which
//     * require access to {@code DB} itself.
//     */
//    Serializer<Object> BASIC = new Serializer<Object>(){
//
//        @Override
//        public void serialize(DataOutput2 out, Object value) throws IOException {
//            __BasicInstance.s.serialize(out,value);
//        }
//
//        @Override
//        public Object deserialize(DataInput2 in, int available) throws IOException {
//            return __BasicInstance.s.deserialize(in,available);
//        }
//
//        @Override
//        public boolean isTrusted() {
//            return true;
//        }
//    };
//

    /**
     * Serialize the content of an object into a ObjectOutput
     *
     * @param out ObjectOutput to save object into
     * @param value Object to serialize
     *
     * @throws java.io.IOException in case of IO error
     */
    void serialize(@NotNull DataOutput2 out, @NotNull A value) throws IOException;


    /**
     * Deserialize the content of an object from a DataInput.
     *
     * @param input to read serialized data from
     * @param available how many bytes are available in DataInput for reading, may be -1 (in streams) or 0 (null).
     * @return deserialized object
     * @throws java.io.IOException in case of IO error
     */
    A deserialize(@NotNull DataInput2 input, int available) throws IOException;

    /**
     * Data could be serialized into record with variable size or fixed size.
     * Some optimizations can be applied to serializers with fixed size
     *
     * @return fixed size or -1 for variable size
     */
    default int fixedSize(){
        return -1;
    }

    /**
     * <p>
     * MapDB has relax record size boundary checking.
     * It expect deserializer to read exactly as many bytes as were writen during serialization.
     * If deserializer reads more bytes it might start reading others record data in store.
     * </p><p>
     * Some serializers (Kryo) have problems with this. To prevent this we can not read
     * data directly from store, but must copy them into separate {@code byte[]}.
     * So zero copy optimalizations is disabled by default, and must be explicitly enabled here.
     * </p><p>
     * This flag indicates if this serializer was 'verified' to read as many bytes as it
     * writes. It should be also much better tested etc.
     * </p>
     *
     * @return true if this serializer is well tested and writes as many bytes as it reads.
     */
    default boolean isTrusted(){
        return false;
    }

    @Override
    default int compare(A o1, A o2) {
        return ((Comparable)o1).compareTo(o2);
    }

    default boolean equals(A a1, A a2){
        return a1==a2 || (a1!=null && a1.equals(a2));
    }

    default int hashCode(@NotNull A a, int seed){
        return DataIO.intHash(a.hashCode()+seed);
    }

    default A valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) throws IOException {
        Object keys = valueArrayDeserialize(input, pos+1);
        return valueArrayGet(keys, pos);
//        A a=null;
//        while(pos-- >= 0){
//            a = deserialize(input, -1);
//        }
//        return a;
    }



    default int valueArrayBinarySearch(A key, DataInput2 input, int keysLen, Comparator comparator) throws IOException {
        Object keys = valueArrayDeserialize(input, keysLen);
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


    default int valueArraySearch(Object keys, A key){
        return Arrays.binarySearch((Object[])keys, key, (Comparator<Object>)this);
    }

    default int valueArraySearch(Object keys, A key, Comparator comparator){
        if(comparator==this)
            return valueArraySearch(keys, key);
        return Arrays.binarySearch((Object[])keys, key, comparator);
    }

    @SuppressWarnings("unchecked")
    default void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        Object[] vals2 = (Object[]) vals;
        for(Object o:vals2){
            serialize(out, (A) o);
        }
    }

    default Object valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        Object[] ret = new Object[size];
        for(int i=0;i<size;i++){
            ret[i] = deserialize(in,-1);
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    default A valueArrayGet(Object vals, int pos){
        return (A) ((Object[])vals)[pos];
    }

    default int valueArraySize(Object vals){
        return ((Object[])vals).length;
    }

    default Object valueArrayEmpty(){
        return new Object[0];
    }

    default Object valueArrayPut(Object vals, int pos, A newValue) {
        return DataIO.arrayPut((Object[]) vals, pos, newValue);
    }


    default Object valueArrayUpdateVal(Object vals, int pos, A newValue) {
        Object[] vals2 = ((Object[])vals).clone();
        vals2[pos] = newValue;
        return vals2;
    }

    default Object valueArrayFromArray(Object[] objects) {
        return objects;
    }

    default Object valueArrayCopyOfRange(Object vals, int from, int to) {
        return Arrays.copyOfRange((Object[])vals, from, to);
    }

    default Object valueArrayDeleteValue(Object vals, int pos) {
        return DataIO.arrayDelete((Object[]) vals, pos, 1);
    }

    default Object[] valueArrayToArray(Object vals){
        Object[] ret = new Object[valueArraySize(vals)];
        for(int i=0;i<ret.length;i++){
            ret[i] = valueArrayGet(vals,i);
        }
        return ret;
    }

    default boolean needsAvailableSizeHint(){
        return false;
    }

}
