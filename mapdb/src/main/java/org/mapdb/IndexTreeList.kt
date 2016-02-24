package org.mapdb

import org.eclipse.collections.api.map.primitive.MutableLongLongMap
import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * [ArrayList] like structure backed by tree
 */
//TODO this is just broken
internal class IndexTreeList<E> (
        val isThreadSafe:Boolean,
        val serializer:Serializer<E>,
        val store:Store,
        val map: MutableLongLongMap
) : AbstractList<E?>() {

    var _size = 0;

    override fun add(element: E?): Boolean {
        val index = _size++
        val recid = store.put(element, serializer)
        map.put(index.toLong(), recid)
        return true
    }

    override fun add(index: Int, element: E?) {
        checkIndex(index)
        //make space
        for(i in size-1 downTo index){
            val recid = map.get(i.toLong())
            if(recid==0L)
                continue;
            map.remove(i.toLong())
            map.put((i+1).toLong(), recid)
        }
        _size++

        val recid = map[index.toLong()]
        if(recid==0L){
            map.put(index.toLong(), store.put(element, serializer))
        }else{
            store.update(recid, element, serializer)
        }

    }

    override fun clear() {
        _size = 0;
        //TODO iterate over map and clear in in single pass if IndexTreeLongLongMap
        map.forEachValue{recid->store.delete(recid, serializer)}
        map.clear()
    }

    override fun removeAt(index: Int): E? {
        checkIndex(index)
        val recid = map[index.toLong()]
        val ret = if(recid==0L){
             null;
        }else {
            val ret = store.get(recid, serializer)
            store.delete(recid, serializer)
            map.remove(index.toLong())
            ret
        }
        //move down rest of the list
        for(i in index+1 until size){
            val recid = map.get(i.toLong())
            if(recid==0L)
                continue;
            map.remove(i.toLong())
            map.put((i-1).toLong(), recid)
        }
        _size--
        return ret;
    }

    override fun set(index: Int, element: E?): E? {
        checkIndex(index)
        val recid = map[index.toLong()]
        if(recid==0L){
            map.put(index.toLong(), store.put(element, serializer))
            return null;
        }else{
            val ret = store.get(recid, serializer)
            store.update(recid, element, serializer)
            return ret
        }
    }

    fun checkIndex(index:Int){
        if(index<0 || index>=_size)
            throw IndexOutOfBoundsException()
    }

    override fun get(index: Int): E? {
        checkIndex(index)

        val recid = map[index.toLong()]
        if(recid==0L){
            return null;
        }
        return store.get(recid, serializer)
    }

    override fun isEmpty(): Boolean {
        return size==0
    }

    //TODO PERF iterate over Map and fill gaps, should be faster. But careful if map is HashMap or not sorted other way
    override fun iterator(): MutableIterator<E?> {
        return object:MutableIterator<E?>{

            var index = 0;
            var indexToRemove:Int?=null;
            override fun hasNext(): Boolean {
                return index <this@IndexTreeList._size
            }

            override fun next(): E? {
                if(!hasNext())
                    throw NoSuchElementException()
                indexToRemove = index
                val ret = this@IndexTreeList[index]
                index++;
                return ret;
            }

            override fun remove() {
                removeAt(indexToRemove ?:throw IllegalStateException())
                index--
                indexToRemove =null
            }

        }
    }


    override val size: Int
        get() = _size


}