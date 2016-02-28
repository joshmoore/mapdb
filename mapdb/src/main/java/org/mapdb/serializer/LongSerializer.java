package org.mapdb.serializer;

import java.util.Arrays;

public abstract class LongSerializer extends EightByteSerializer<Long> {

        @Override
        protected Long unpack(long l) {
            return new Long(l);
        }

        @Override
        protected long pack(Long l) {
            return l.longValue();
        }

        @Override
        public int valueArraySearch(Object keys, Long key) {
            return Arrays.binarySearch((long[])keys, key);
        }

}
