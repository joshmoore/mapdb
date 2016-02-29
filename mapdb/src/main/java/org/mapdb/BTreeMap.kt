package org.mapdb

import org.eclipse.collections.api.list.primitive.MutableLongList
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.eclipse.collections.impl.stack.mutable.primitive.LongArrayStack
import org.mapdb.BTreeMapJava.*
import org.mapdb.serializer.GroupSerializer
import org.mapdb.serializer.GroupSerializerObjectArray
import java.io.Closeable
import java.io.ObjectStreamException
import java.io.PrintStream
import java.io.Serializable
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.locks.LockSupport
import java.util.function.BiConsumer

/**
 * Concurrent sorted BTree Map
 */
class BTreeMap<K,V>(
        keySerializer:Serializer<K>,
        valueSerializer:Serializer<V>,
        val rootRecidRecid:Long,
        val store:Store,
        val maxNodeSize:Int,
        val comparator:Comparator<K>,
        val threadSafe:Boolean,
        val counterRecid:Long,
        override val hasValues:Boolean = true
):Verifiable, Closeable, Serializable,
        ConcurrentNavigableMap<K, V>, ConcurrentNavigableMapExtra<K,V> {

    override val keySerializer:GroupSerializer<K,Any?> = keySerializer as GroupSerializer<K,Any?>
    override val valueSerializer:GroupSerializer<V,Any?> = valueSerializer  as GroupSerializer<V,Any?>


    companion object {
        fun <K, V> make(
                keySerializer: Serializer<K> = Serializer.JAVA as Serializer<K>,
                valueSerializer: Serializer<V> = Serializer.JAVA as Serializer<V>,
                store: Store = StoreTrivial(),
                rootRecidRecid: Long = //insert recid of new empty node
                putEmptyRoot(store, keySerializer as GroupSerializer<K,Any?>, valueSerializer as GroupSerializer<V,Any?>),
                maxNodeSize: Int =  CC.BTREEMAP_MAX_NODE_SIZE ,
                comparator: Comparator<K> = keySerializer,
                threadSafe:Boolean = true,
                counterRecid:Long=0L
            ) =
                BTreeMap(
                        keySerializer = keySerializer,
                        valueSerializer = valueSerializer,
                        store = store,
                        rootRecidRecid = rootRecidRecid,
                        maxNodeSize = maxNodeSize,
                        comparator = comparator,
                        threadSafe = threadSafe,
                        counterRecid = counterRecid
                )

        internal fun <K, V> putEmptyRoot(store: Store, keySerializer: GroupSerializer<K, *>, valueSerializer: GroupSerializer<V,*>): Long {
            return store.put(
                    store.put(
                            Node(LEFT + RIGHT, 0L, keySerializer.valueArrayEmpty(),
                                    valueSerializer.valueArrayEmpty(), keySerializer, valueSerializer),
                            NodeSerializer(keySerializer, valueSerializer)),
                    Serializer.RECID)
        }


        internal val NO_VAL_SERIALIZER = object: GroupSerializer<Boolean,Int>{

            override fun valueArrayCopyOfRange(vals: Int?, from: Int, to: Int): Int? {
                return to-from;
            }

            override fun valueArrayDeleteValue(vals: Int, pos: Int): Int? {
                return vals-1
            }

            override fun valueArrayDeserialize(`in`: DataInput2?, size: Int): Int? {
                return size
            }

            override fun valueArrayEmpty(): Int? {
                return 0
            }

            override fun valueArrayFromArray(objects: Array<out Any>?): Int? {
                throw IllegalAccessError()
            }

            override fun valueArrayGet(vals: Int?, pos: Int): Boolean? {
                return java.lang.Boolean.TRUE
            }

            override fun valueArrayPut(vals: Int?, pos: Int, newValue: Boolean?): Int? {
                return vals!! + 1
            }

            override fun valueArraySearch(keys: Int?, key: Boolean?): Int {
                throw IllegalAccessError()
            }

            override fun valueArraySearch(keys: Int?, key: Boolean?, comparator: Comparator<*>?): Int {
                throw IllegalAccessError()
            }

            override fun valueArraySerialize(out: DataOutput2?, vals: Int?) {
            }

            override fun valueArraySize(vals: Int?): Int {
                return vals!!
            }

            override fun valueArrayUpdateVal(vals: Int?, pos: Int, newValue: Boolean?): Int? {
                return vals
            }

            override fun deserialize(input: DataInput2, available: Int): Boolean? {
                throw IllegalAccessError();
            }

            override fun serialize(out: DataOutput2, value: Boolean) {
                throw IllegalAccessError();
            }

            override fun isTrusted(): Boolean {
                return true
            }
        }
    }

    private val hasBinaryStore = store is StoreBinary

    internal val nodeSerializer = NodeSerializer(this.keySerializer, this.valueSerializer);

    internal val rootRecid: Long
        get() = store.get(rootRecidRecid, Serializer.RECID)
                ?: throw DBException.DataCorruption("Root Recid not found");

    /** recids of left-most nodes in tree */
    internal val leftEdges: MutableLongList = {
        val ret = LongArrayList()

        var recid = rootRecid
        while (true) {
            val node = getNode(recid)
            if (CC.ASSERT && recid <= 0L)
                throw AssertionError()
            ret.add(recid)
            if (node.isDir.not())
                break
            recid = node.children[0]
        }

        ret.toReversed().asSynchronized()
    }()

    private val locks = ConcurrentHashMap<Long, Long>()

    override operator fun get(key: K?): V? {
        if (key == null)
            throw NullPointerException()

        return if (hasBinaryStore) getBinary(key)
            else getNonBinary(key)
    }


    private fun getBinary(key: K): V? {
        val binary = store as StoreBinary

        var current = rootRecid

        val binaryGet = BinaryGet<K, V>(keySerializer, valueSerializer, comparator, key)

        do {
            current = binary.getBinaryLong(current, binaryGet)
        } while (current != -1L)

        return binaryGet.value;

    }


    private fun getNonBinary(key: K?): V? {
        var current = rootRecid
        var A = getNode(current)

        //dive into bottom
        while (A.isDir) {
            current = findChild(keySerializer, A, comparator, key)
            A = getNode(current)
        }

        //follow link until necessary
        var ret = leafGet(A, comparator, key, keySerializer, valueSerializer)
        while (LINK == ret) {
            current = A.link;
            A = getNode(current)
            ret = leafGet(A, comparator, key, keySerializer, valueSerializer)
        }
        return ret as V?;
    }

    override fun put(key: K?, value: V?): V? {
        if (key == null || value == null)
            throw NullPointerException()
        return put2(key, value, false)
    }

    protected fun put2(key: K, value: V, onlyIfAbsent: Boolean): V? {
        if (key == null || value == null)
            throw NullPointerException()

        try {
            var v = key!!
            var completed = false
            val stack = LongArrayStack()
            val rootRecid = rootRecid

            var current = rootRecid

            var A = getNode(current)
            while (A.isDir) {
                var t = current
                current = findChild(keySerializer, A, comparator, v)
                if (current != A.link) {
                    stack.push(t)
                }
                A = getNode(current)
            }

            var level = 1
            var p = 0L
            do {

                leafLink@ while (true) {
                    lock(current)

                    A = getNode(current)

                    //follow link, until key is higher than highest key in node
                    if (!A.isRightEdge && comparator.compare(v, A.highKey(keySerializer) as K) > 0) {
                        //TODO PERF optimize
                        //key is greater, load next link
                        unlock(current)
                        current = A.link
                        continue@leafLink
                    }
                    break@leafLink
                }

                //current node is locked, and its highest value is higher/equal to key
                var pos = keySerializer.valueArraySearch(A.keys, v, comparator)
                if (pos >= 0) {
                    //entry exist in current node, so just update
                    pos = pos - 1 + A.intLeftEdge();
                    val linkValue = (!A.isLastKeyDouble && pos>=valueSerializer.valueArraySize(A.values))
                    //key exist in node, just update
                    val oldValue =
                            if(linkValue) null
                            else valueSerializer.valueArrayGet(A.values, pos)

                    //update only if not exist, return
                    if (!onlyIfAbsent || linkValue) {
                        val values =
                                if(linkValue) valueSerializer.valueArrayPut(A.values, pos, value)
                                else valueSerializer.valueArrayUpdateVal(A.values, pos, value)
                        var flags = A.flags.toInt();
                        if(linkValue){
                            counterIncrement(1)
                            if(CC.ASSERT && A.isLastKeyDouble)
                                throw AssertionError()
                            //duplicate last key by adding flag
                            flags += LAST_KEY_DOUBLE
                        }
                        A = Node(flags, A.link, A.keys, values, keySerializer, valueSerializer)
                        store.update(current, A, nodeSerializer)
                    }
                    unlock(current)
                    return oldValue
                }

                //normalise pos
                pos = -pos - 1

                //key does not exist, node must be expanded
                A = if (A.isDir) copyAddKeyDir(A, pos, v, p)
                else{
                    counterIncrement(1)
                    copyAddKeyLeaf(A, pos, v, value)
                }
                val keysSize = keySerializer.valueArraySize(A.keys) + A.intLastKeyTwice()
                if (keysSize < maxNodeSize) {
                    //it is safe to insert without spliting
                    store.update(current, A, nodeSerializer)
                    unlock(current)
                    return null
                }

                //node is not safe it requires split
                val splitPos = keysSize / 2
                val B = copySplitRight(A, splitPos)
                val q = store.put(B, nodeSerializer)
                A = copySplitLeft(A, splitPos, q)
                store.update(current, A, nodeSerializer)

                if (current != rootRecid) {
                    //is not root
                    unlock(current)
                    p = q
                    v = A.highKey(keySerializer) as K
                    //                if(CC.ASSERT && COMPARATOR.compare(v, key)<0)
                    //                    throw AssertionError()
                    level++
                    current = if (stack.isEmpty.not()) {
                        stack.pop()
                    } else {
                        //pointer to left most node at level
                        leftEdges.get(level - 1)
                    }
                } else {
                    //is root
                    val R = Node(
                            DIR + LEFT + RIGHT,
                            0L,
                            keySerializer.valueArrayFromArray(arrayOf(A.highKey(keySerializer) as Any?)),
                            longArrayOf(current, q),
                            keySerializer,
                            valueSerializer
                    )

                    unlock(current)
                    lock(rootRecidRecid)
                    val newRootRecid = store.put(R, nodeSerializer)
                    leftEdges.add(newRootRecid)
                    //TODO there could be a race condition between leftEdges  update and rootRecidRef update. Investigate!
                    store.update(rootRecidRecid, newRootRecid, Serializer.RECID)

                    unlock(rootRecidRecid)

                    return null;
                }

            } while (!completed)

            return null

        } catch(e: Throwable) {
            unlockAllCurrentThread()
            throw e
        } finally {
            if (CC.ASSERT)
                assertCurrentThreadUnlocked()
        }
    }

    override fun remove(key: K?): V? {
        if (key == null)
            throw NullPointerException()

        return removeOrReplace(key, null, null)
    }

    protected fun removeOrReplace(key: K, expectedOldValue: V?, replaceWithValue: V?): V? {
        if (key == null)
            throw NullPointerException()

        try {
            val v = key

            val rootRecid = rootRecid

            var current = rootRecid

            var A = getNode(current)
            while (A.isDir) {
                current = findChild(keySerializer, A, comparator, v)
                A = getNode(current)
            }

            leafLink@ while (true) {
                lock(current)

                A = getNode(current)

                //follow link, until key is higher than highest key in node
                if (!A.isRightEdge && comparator.compare(v, A.highKey(keySerializer) as K) > 0) {
                    //key is greater, load next link
                    unlock(current)
                    current = A.link
                    continue@leafLink
                }
                break@leafLink
            }

            //current node is locked, and its highest value is higher/equal to key
            val pos = keySerializer.valueArraySearch(A.keys, v, comparator)
            var oldValue: V? = null
            val keysSize = keySerializer.valueArraySize(A.keys);
            if (pos >= 1 - A.intLeftEdge() && pos < keysSize - 1 + A.intRightEdge() + A.intLastKeyTwice()) {
                val valuePos = pos - 1 + A.intLeftEdge();
                //key exist in node, just update
                oldValue = valueSerializer.valueArrayGet(A.values, valuePos)
                var keys = A.keys
                var flags = A.flags.toInt()
                if (expectedOldValue == null || valueSerializer.equals(expectedOldValue!!, oldValue)) {
                    val values = if (replaceWithValue == null) {
                        //remove
                        if (A.isLastKeyDouble && pos == keysSize - 1) {
                            //last value is twice in node, but should be removed from here
                            // instead of removing key, just unset flag
                            flags -= LAST_KEY_DOUBLE
                        } else {
                            keys = keySerializer.valueArrayDeleteValue(A.keys, pos + 1)
                        }
                        counterIncrement(-1)
                        valueSerializer.valueArrayDeleteValue(A.values, valuePos + 1)
                    } else {
                        //replace value, do not modify keys
                        valueSerializer.valueArrayUpdateVal(A.values, valuePos, replaceWithValue)
                    }

                    A = Node(flags, A.link, keys, values, keySerializer, valueSerializer)
                    store.update(current, A, nodeSerializer)
                } else {
                    oldValue = null
                }
            }
            unlock(current)

            return oldValue
        } catch(e: Throwable) {
            unlockAllCurrentThread()
            throw e
        } finally {
            if (CC.ASSERT)
                assertCurrentThreadUnlocked()
        }
    }


    private fun copySplitLeft(a: Node, splitPos: Int, link: Long): Node {
        var flags = a.intDir() * DIR + a.intLeftEdge() * LEFT + LAST_KEY_DOUBLE * (1 - a.intDir())

        var keys = keySerializer.valueArrayCopyOfRange(a.keys, 0, splitPos)
        //        if(!a.isDir) {
        //            val keysSize = keySerializer.valueArraySize(keys)
        //            val oneBeforeLast = keySerializer.valueArrayGet(keys, keysSize-2)
        //            keys = keySerializer.valueArrayUpdateVal(keys, keysSize-1, oneBeforeLast)
        //        }
        val valSplitPos = splitPos - 1 + a.intLeftEdge();
        val values = if (a.isDir) {
            val c = a.values as LongArray
            Arrays.copyOfRange(c, 0, valSplitPos)
        } else {
            valueSerializer.valueArrayCopyOfRange(a.values, 0, valSplitPos)
        }

        return Node(flags, link, keys, values, keySerializer, valueSerializer)

    }

    private fun copySplitRight(a: Node, splitPos: Int): Node {
        val flags = a.intDir() * DIR + a.intRightEdge() * RIGHT + a.intLastKeyTwice() * LAST_KEY_DOUBLE

        val keys = keySerializer.valueArrayCopyOfRange(a.keys, splitPos - 1, keySerializer.valueArraySize(a.keys))

        val valSplitPos = splitPos - 1 + a.intLeftEdge();
        val values = if (a.isDir) {
            val c = a.values as LongArray
            Arrays.copyOfRange(c, valSplitPos, c.size)
        } else {
            val size = valueSerializer.valueArraySize(a.values)
            valueSerializer.valueArrayCopyOfRange(a.values, valSplitPos, size)
        }

        return Node(flags, a.link, keys, values, keySerializer, valueSerializer)
    }


    private fun copyAddKeyLeaf(a: Node, insertPos: Int, key: K, value: V): Node {
        if (CC.ASSERT && a.isDir)
            throw AssertionError()

        val keys = keySerializer.valueArrayPut(a.keys, insertPos, key)

        val valuesInsertPos = insertPos - 1 + a.intLeftEdge();
        val values = valueSerializer.valueArrayPut(a.values, valuesInsertPos, value)

        return Node(a.flags.toInt(), a.link, keys, values, keySerializer, valueSerializer)
    }

    private fun copyAddKeyDir(a: Node, insertPos: Int, key: K, newChild: Long): Node {
        if (CC.ASSERT && a.isDir.not())
            throw AssertionError()

        val keys = keySerializer.valueArrayPut(a.keys, insertPos, key)

        val values = arrayPut(a.values as LongArray, insertPos + a.intLeftEdge(), newChild)

        return Node(a.flags.toInt(), a.link, keys, values, keySerializer, valueSerializer)
    }


    fun lock(nodeRecid: Long) {
        if(!threadSafe)
            return
        val value = Thread.currentThread().id
        //try to lock, but only if current node is not empty
        while (locks.putIfAbsent(nodeRecid, value) != null)
            LockSupport.parkNanos(10)
    }

    fun unlock(nodeRecid: Long) {
        if(!threadSafe)
            return
        val v = locks.remove(nodeRecid)
        if (v == null || v != Thread.currentThread().id)
            throw AssertionError("Unlocked wrong thread");
    }

    fun unlockAllCurrentThread() {
        if(!threadSafe)
            return
        val id = Thread.currentThread().id
        val iter = locks.iterator()
        while (iter.hasNext()) {
            val e = iter.next()
            if (e.value == id) {
                iter.remove()
            }
        }
    }


    fun assertCurrentThreadUnlocked() {
        if(!threadSafe)
            return
        val id = Thread.currentThread().id
        val iter = locks.iterator()
        while (iter.hasNext()) {
            val e = iter.next()
            if (e.value == id) {
                throw AssertionError("Node is locked: " + e.key)
            }
        }
    }

    override fun close() {
        store.close()
    }


    override fun verify() {
        fun verifyRecur(node: Node, left: Boolean, right: Boolean, knownNodes: LongHashSet, nextNodeRecid: Long) {
            if (left != node.isLeftEdge)
                throw AssertionError("left does not match $left")
            //TODO follow link for this assertion
//            if (right != node.isRightEdge)
//                throw AssertionError("right does not match $right")

            //check keys are sorted, no duplicates
            val keysLen = keySerializer.valueArraySize(node.keys)
            for (i in 1 until keysLen) {
                val compare = comparator.compare(
                        keySerializer.valueArrayGet(node.keys, i - 1),
                        keySerializer.valueArrayGet(node.keys, i))
                if (compare >= 0)
                    throw AssertionError("Not sorted: " + Arrays.toString(keySerializer.valueArrayToArray(node.keys)))
            }

            //iterate over child
            if (node.isDir) {
                val child = node.values as LongArray
                var prevLink = 0L;
                for (i in child.size - 1 downTo 0) {
                    val recid = child[i]

                    if (knownNodes.contains(recid))
                        throw AssertionError("recid duplicate: $recid")
                    knownNodes.add(recid)
                    var node = getNode(recid)
                    verifyRecur(node, left = (i == 0) && left, right = (child.size == i + 1) && right,
                            knownNodes = knownNodes, nextNodeRecid = nextNodeRecid)

                    //TODO implement follow link
                    //                    //follow link until next node is found
                    //                    while(node.link!=prevLink){
                    //                        if(knownNodes.contains(node.link))
                    //                            throw AssertionError()
                    //                        knownNodes.add(node.link)
                    //
                    //                        node = getNode(node.link)
                    //
                    //                        verifyRecur(node, left = false, right= node.link==0L,
                    //                                knownNodes = knownNodes, nextNodeRecid=prevLink)
                    //                    }
                    prevLink = recid
                }
            }
        }


        val rootRecid = rootRecid
        val node = getNode(rootRecid)

        val knownNodes = LongHashSet.newSetWith(rootRecid)

        verifyRecur(node, left = true, right = true, knownNodes = knownNodes, nextNodeRecid = 0L)

        //verify that linked nodes share the same keys on their edges
        for (leftRecid in leftEdges.toArray()) {

            if (knownNodes.contains(leftRecid).not())
                throw AssertionError()
            var node = getNode(leftRecid)
            if (!knownNodes.remove(leftRecid))
                throw AssertionError()

            while (node.isRightEdge.not()) {
                //TODO enable once links are traced
                //                if(!knownNodes.remove(node.link))
                //                    throw AssertionError()

                val next = getNode(node.link)
                if (comparator.compare(node.highKey(keySerializer) as K, keySerializer.valueArrayGet(next.keys, 0)) != 0)
                    throw AssertionError(node.link)

                node = next
            }
        }
        //TODO enable once links are traced
        //        if(knownNodes.isEmpty.not())
        //            throw AssertionError(knownNodes)
    }


    private fun getNode(nodeRecid: Long) =
            store.get(nodeRecid, nodeSerializer)
                    ?: throw DBException.DataCorruption("Node not found")

    private fun nodeToString(nodeRecid:Long?, node:Node):String{
        var str = if (node.isDir) "DIR " else "LEAF "

        if (node.isLeftEdge)
            str += "L"
        if (node.isRightEdge)
            str += "R"
        if (node.isLastKeyDouble)
            str += "D"
        str += " recid=$nodeRecid, link=${node.link}, keys=" + Arrays.toString(keySerializer.valueArrayToArray(node.keys)) + ", "


        str +=
                if (node.isDir) "child=" + Arrays.toString(node.children)
                else "vals=" + Arrays.toString(valueSerializer.valueArrayToArray(node.values))
        return str
    }

    fun printStructure(out: PrintStream) {
        fun printRecur(nodeRecid: Long, prefix: String) {
            val node = getNode(nodeRecid);

            out.println(prefix + nodeToString(nodeRecid, node))

            if (node.isDir) {
                node.children.forEach {
                    printRecur(it, "  " + prefix)
                }
            }
        }

        printRecur(rootRecid, "")

    }


    override fun putAll(from: Map<out K?, V?>) {
        for (e in from.entries) {
            put(e.key, e.value)
        }
    }

    override fun putIfAbsentBoolean(key: K?, value: V?): Boolean {
        if (key == null || value == null)
            throw NullPointerException()
        return putIfAbsent(key, value) != null
    }

    override fun putIfAbsent(key: K?, value: V?): V? {
        if (key == null || value == null)
            throw NullPointerException()
        return put2(key, value, true)
    }

    override fun remove(key: Any?, value: Any?): Boolean {
        if (key == null || value == null)
            throw NullPointerException()
        return removeOrReplace(key as K, value as V, null) != null
    }

    override fun replace(key: K?, oldValue: V?, newValue: V?): Boolean {
        if (key == null || oldValue == null || newValue == null)
            throw NullPointerException()
        return removeOrReplace(key, oldValue, newValue) != null
    }

    override fun replace(key: K?, value: V?): V? {
        if (key == null || value == null)
            throw NullPointerException()
        return removeOrReplace(key, null, value)
    }

    override fun clear() {
        //TODO PERF optimize, traverse nodes and clear each node in one step
        val iter = keys.iterator();
        while (iter.hasNext()) {
            iter.next()
            iter.remove()
        }
    }

    override fun containsKey(key: K): Boolean {
        return get(key) != null
    }

    override fun containsValue(value: V): Boolean {
        return values.contains(value)
    }

    override fun isEmpty(): Boolean {
        return keys.iterator().hasNext().not()
    }

    override val size: Int
        get() = Math.min(Int.MAX_VALUE.toLong(), sizeLong()).toInt()

    override fun sizeLong(): Long {
        if(counterRecid!=0L)
            return store.get(counterRecid, Serializer.LONG)
                    ?:throw DBException.DataCorruption("Counter not found")

        var ret = 0L
        val iter = keys.iterator()
        while (iter.hasNext()) {
            iter.next()
            ret++
        }
        return ret
    }

    private fun counterIncrement(i:Int){
        if(counterRecid==0L)
            return
        do{
            val counter = store.get(counterRecid, Serializer.LONG)
                    ?:throw DBException.DataCorruption("Counter not found")
        }while(store.compareAndSwap(counterRecid, counter, counter+i, Serializer.LONG).not())
    }


    private val descendingMap = DescendingMap(this, null, true, null, false)

    override fun descendingKeySet(): NavigableSet<K>? {
        return descendingMap.navigableKeySet()
    }

    override fun descendingMap(): ConcurrentNavigableMap<K, V>? {
        return descendingMap;
    }

    //TODO retailAll etc should use serializers for comparasions, remove AbstractSet and AbstractCollection completely
    //TODO PERF replace iterator with forEach, much faster indexTree traversal
    override val entries: MutableSet<MutableMap.MutableEntry<K, V?>> = object : AbstractSet<MutableMap.MutableEntry<K, V?>>() {

        override fun add(element: MutableMap.MutableEntry<K, V?>): Boolean {
            this@BTreeMap.put(element.key, element.value)
            return true
        }


        override fun clear() {
            this@BTreeMap.clear()
        }

        override fun iterator(): MutableIterator<MutableMap.MutableEntry<K, V?>> {
            return this@BTreeMap.entryIterator()
        }

        override fun remove(element: MutableMap.MutableEntry<K, V?>?): Boolean {
            if (element == null || element.key == null || element.value == null)
                throw NullPointerException()
            return this@BTreeMap.remove(element.key as Any, element.value)
        }


        override fun contains(element: MutableMap.MutableEntry<K, V?>): Boolean {
            val v = this@BTreeMap.get(element.key)
                    ?: return false
            val value = element.value
                    ?: return false
            return valueSerializer.equals(value, v)
        }

        override fun isEmpty(): Boolean {
            return this@BTreeMap.isEmpty()
        }

        override val size: Int
            get() = this@BTreeMap.size

    }

    override fun navigableKeySet(): NavigableSet<K?> {
        return keys;
    }

    override val keys: NavigableSet<K?> =
            KeySet<K>(this as ConcurrentNavigableMap2<K, Any>, this.hasValues)



    override val values: MutableCollection<V?> = object : AbstractCollection<V>() {

        override fun clear() {
            this@BTreeMap.clear()
        }

        override fun isEmpty(): Boolean {
            return this@BTreeMap.isEmpty()
        }

        override val size: Int
            get() = this@BTreeMap.size


        override fun iterator(): MutableIterator<V?> {
            return this@BTreeMap.valueIterator()
        }

        override fun contains(element: V): Boolean {
            if (element == null)
                throw NullPointerException()
            return super.contains(element)
        }
    }

    abstract class BTreeIterator<K,V>(val m:BTreeMap<K,V>){

        protected var currentPos = -1
        protected var currentLeaf:Node? = null
        protected var lastReturnedKey: K? = null

        init{
            advanceFrom(m.leftEdges.first!!)
        }


        fun hasNext():Boolean = currentLeaf!=null

        fun remove() {
            m.remove(lastReturnedKey ?: throw IllegalStateException())
            this.lastReturnedKey = null
        }


        private fun advanceFrom(recid: Long) {
            var node: Node? =
                    if(recid==0L) null
                    else m.getNode(recid);
            // iterate until node is not empty or link is not found
            while (node != null && m.keySerializer.valueArraySize(node.keys)+node.intLastKeyTwice() == 2 - node.intLeftEdge() - node.intRightEdge()) {
                node =
                        if (node.isRightEdge) null
                        else m.getNode(node.link)
            }
            //set leaf
            currentLeaf = node
            currentPos = if (node == null) -1 else 1 - node.intLeftEdge()
        }

        protected fun advance(){
            val currentLeaf:Node = currentLeaf?:return
            lastReturnedKey = m.keySerializer.valueArrayGet(currentLeaf.keys, currentPos) as K
            currentPos++

            if(currentPos == m.keySerializer.valueArraySize(currentLeaf.keys)-1+currentLeaf.intRightEdge()+currentLeaf.intLastKeyTwice()){
                //reached end of current node, iterate to next
                advanceFrom(currentLeaf.link)
            }
        }
    }


    abstract class BTreeBoundIterator<K, V>(
            val m: BTreeMap<K, V>,
            val lo:K?,
            val loInclusive:Boolean,
            val hi:K?,
            val hiInclusive:Boolean) {

        protected val hiC = if(hiInclusive) 0 else -1
        protected var currentPos = -1
        protected var currentLeaf: Node? = null
        protected var lastReturnedKey: K? = null

        init {
            if(lo==null)
                advanceFrom(m.leftEdges.first!!)
            else
                advanceFromLo()
        }


        fun hasNext(): Boolean = currentLeaf != null

        fun remove() {
            m.remove(lastReturnedKey ?: throw IllegalStateException())
            this.lastReturnedKey = null
        }


        private fun advanceFrom(recid: Long) {
            var node: Node? =
                    if (recid == 0L) null
                    else m.getNode(recid);
            // iterate until node is not empty or link is not found
            while (node != null && m.keySerializer.valueArraySize(node.keys) + node.intLastKeyTwice() == 2 - node.intLeftEdge() - node.intRightEdge()) {
                node =
                        if (node.isRightEdge) null
                        else m.getNode(node.link)
            }
            //set leaf
            currentLeaf = node
            currentPos = if (node == null) -1 else 1 - node.intLeftEdge()

            checkHiBound()
        }

        private fun advanceFromLo() {
            val key = lo
            var current = m.rootRecid
            var A = m.getNode(current)

            //dive into bottom
            while (A.isDir) {
                current = findChild(m.keySerializer, A, m.comparator, key)
                A = m.getNode(current)
            }

            //follow link until necessary
            while(true){
                var pos = m.keySerializer.valueArraySearch(A.keys, key, m.comparator)
                if(!loInclusive && pos>=1-A.intLeftEdge())
                    pos++

                if(pos<0)
                    pos = -pos-1

                if(pos==0 && !A.isLeftEdge)
                    pos++

                //check if is last key
                if(pos< m.keySerializer.valueArraySize(A.keys)-1+A.intLastKeyTwice()+A.intRightEdge()){
                    currentLeaf = A
                    currentPos = pos;
                    checkHiBound()
                    return
                }

                if(A.isRightEdge){
                    //reached end, cancel iteration
                    currentLeaf = null
                    return
                }
                //load next node
                A = m.getNode(A.link)
            }

        }


        protected fun advance() {
            val currentLeaf: Node = currentLeaf ?: return
            lastReturnedKey = m.keySerializer.valueArrayGet(currentLeaf.keys, currentPos) as K
            currentPos++

            if (currentPos == m.keySerializer.valueArraySize(currentLeaf.keys) - 1 + currentLeaf.intRightEdge() + currentLeaf.intLastKeyTwice()) {
                //reached end of current node, iterate to next
                advanceFrom(currentLeaf.link)
            }

            checkHiBound()
        }

        protected fun checkHiBound(){
            if(hi==null)
                return;
            val leaf = currentLeaf
                    ?:return
            val currKey = m.keySerializer.valueArrayGet(leaf.keys, currentPos)
            if(m.comparator.compare(currKey, hi)>hiC){
                //reached end, so cancel iteration
                currentLeaf = null
                currentPos = -1;
            }
        }
    }

    fun entryIterator(): MutableIterator<MutableMap.MutableEntry<K, V?>> {
        return object : BTreeIterator<K, V>(this), MutableIterator<MutableMap.MutableEntry<K, V?>> {
            override fun next(): MutableMap.MutableEntry<K, V?> {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val key = keySerializer.valueArrayGet(leaf.keys, currentPos)
                val value = valueSerializer.valueArrayGet(leaf.values, currentPos - 1 + leaf.intLeftEdge())
                advance()
                return btreeEntry(key, value)
            }
        }
    }


    override fun entryIterator(lo:K?,loInclusive:Boolean,hi:K?,hiInclusive:Boolean): MutableIterator<MutableMap.MutableEntry<K, V?>> {
        return object : BTreeBoundIterator<K, V>(this, lo, loInclusive, hi, hiInclusive), MutableIterator<MutableMap.MutableEntry<K, V?>> {
            override fun next(): MutableMap.MutableEntry<K, V?> {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val key = keySerializer.valueArrayGet(leaf.keys, currentPos)
                val value = valueSerializer.valueArrayGet(leaf.values, currentPos - 1 + leaf.intLeftEdge())
                advance()
                return btreeEntry(key, value)
            }
        }
    }

    override fun keyIterator(): MutableIterator<K> {
        return object : BTreeIterator<K, V>(this), MutableIterator<K> {
            override fun next(): K {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val key = keySerializer.valueArrayGet(leaf.keys, currentPos)
                advance()
                return key
            }
        }
    }

    override fun keyIterator(lo:K?,loInclusive:Boolean,hi:K?,hiInclusive:Boolean): MutableIterator<K> {
        return object : BTreeBoundIterator<K, V>(this, lo, loInclusive, hi, hiInclusive), MutableIterator<K> {
            override fun next(): K {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val key = keySerializer.valueArrayGet(leaf.keys, currentPos)
                advance()
                return key
            }
        }
    }

    fun valueIterator(): MutableIterator<V?> {
        return object : BTreeIterator<K, V>(this), MutableIterator<V?> {
            override fun next(): V? {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val value = valueSerializer.valueArrayGet(leaf.values, currentPos - 1 + leaf.intLeftEdge())
                advance()
                return value
            }
        }
    }

    override fun valueIterator(lo:K?,loInclusive:Boolean,hi:K?,hiInclusive:Boolean): MutableIterator<V> {
        return object : BTreeBoundIterator<K, V>(this, lo, loInclusive, hi, hiInclusive), MutableIterator<V> {
            override fun next(): V {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val value = valueSerializer.valueArrayGet(leaf.values, currentPos - 1 + leaf.intLeftEdge())
                advance()
                return value
            }
        }
    }

    fun descendingLeafIterator(hi: K?):Iterator<Node>{

        class INode(
            var linked:Deque<Node> = LinkedList(),
            var node:Node,
            var nodePos:Int,
            var linkLimit:Long
        ){}


        return object : Iterator<Node>{

            var nextNode:Node? = null;
            val stack:Deque<INode> = LinkedList()
            val leafLinkedStack:Deque<Node> = LinkedList()
            var linkRecidLimit = 0L

            init{
                if(hi!=null){
                    init(hi)
                }else
                    init()
            }

            fun init(){
                var node = getNode(rootRecid)
                while(node.isDir){
                    val linkedStack = LinkedList<Node>()
                    while(node.isRightEdge.not()){
                        linkedStack.add(node)
                        node = getNode(node.link)
                    }
                    val inode = INode(
                            linked=linkedStack,
                            node=node,
                            nodePos = node.children.size-2,
                            linkLimit = linkRecidLimit)

                    stack.add(inode)

                    linkRecidLimit = node.children[node.children.size-1]
                    node = getNode(linkRecidLimit)

                }

                //fill leafLinkedStack
                while(node.isRightEdge.not()){
                    leafLinkedStack.add(node)
                    node = getNode(node.link)
                }
                nextNode = node;
            }

            fun init(hi:K){
                var node = getNode(rootRecid)
                while(node.isDir){
                    var pos = keySerializer.valueArraySearch(node.keys, hi, comparator)
                    if(pos<0)
                        pos=-pos-1
                    pos += -1 + node.intLeftEdge()

                    val linkedStack = LinkedList<Node>()

                    //follow link until needed
                    while(pos>=keySerializer.valueArraySize(node.keys) && node.link!=0L){
                        linkedStack.add(node)
                        node = getNode(node.link)
                        pos = keySerializer.valueArraySearch(node.keys, hi, comparator)
                        if(pos<0)
                            pos=-pos-1
                        pos += -1 + node.intLeftEdge()
                    }
                    val inode = INode(
                            linked=linkedStack,
                            node=node,
                            nodePos = pos-1,
                            linkLimit = linkRecidLimit)

                    stack.add(inode)

                    linkRecidLimit = node.children[Math.min(pos, node.children.size-1)]
                    node = getNode(linkRecidLimit)
                }
                var pos = keySerializer.valueArraySearch(node.keys, hi, comparator)
                if(pos<0)
                    pos=-pos-1
                pos += -1 + node.intLeftEdge()

                //fill leafLinkedStack
                while(pos>=keySerializer.valueArraySize(node.keys) && node.link!=0L){
                    leafLinkedStack.add(node)
                    node = getNode(node.link)
                    pos = keySerializer.valueArraySearch(node.keys, hi, comparator)
                    if(pos<0)
                        pos=-pos-1
                    pos += -1 + node.intLeftEdge()
                }
                nextNode = node;
            }


            fun advance(){
                //try to get previous value from linked leaf
                nextNode = leafLinkedStack.pollFirst();
                if(nextNode!=null)
                    return
                if(CC.ASSERT && leafLinkedStack.isEmpty().not())
                    throw AssertionError()

                fun stackMove(){
                    if(stack.isEmpty() || stack.last.nodePos>-1) {
                        return
                    }

                    //try to move on linked
                    val linked = stack.last.linked.pollLast()
                    if(linked!=null){
                        stack.last.node = linked
                        stack.last.nodePos = linked.children.size-1
                        return
                    }
                    val limit = stack.last().linkLimit
                    stack.pollLast()
                    //try upper levels
                    stackMove()
                    if(stack.isEmpty()){
                        return
                    }

                    val linkedStack = LinkedList<Node>()
                    val nodeRecid = stack.last.node.children[stack.last.nodePos--]
                    var node = getNode(nodeRecid)
                    while(node.link != limit){
                        if(CC.ASSERT && hi!=null && comparator.compare(hi,keySerializer.valueArrayGet(node.keys, 0))<0){
                            throw AssertionError()
                        }
                        linkedStack.add(node)
                        node = getNode(node.link)
                    }
                    val inode = INode(
                            linked = linkedStack,
                            node=node,
                            nodePos = node.children.size-1,
                            linkLimit=nodeRecid
                        )
                    stack.add(inode)

                }

                stackMove()

                if(stack.isEmpty()){
                    //terminate iteration
                    nextNode = null
                    return;
                }

                //no more leaf records, ascend one level and find previous leaf
                val inode = stack.last
                val childRecid = inode.node.children[inode.nodePos--]

                var node = getNode(childRecid)

                //follow link at leaf level, until linkRecidLimit
                while(node.link!=linkRecidLimit){
                    leafLinkedStack.add(node)
                    node = getNode(node.link)
                }
                nextNode = node;
                //start of this linked sequence becomes new limit
                linkRecidLimit = childRecid
            }

            override fun hasNext(): Boolean {
                return nextNode != null
            }

            override fun next(): Node {
                val ret = nextNode
                        ?: throw NoSuchElementException()
                advance()
                if(CC.ASSERT && nextNode!=null){
                    val currKey = keySerializer.valueArrayGet(ret.keys, 0)
                    val nextKey = nextNode!!.highKey(keySerializer)
                    if(comparator.compare(nextKey, currKey)>0){
                        throw AssertionError("wrong reverse iteration")
                    }
                }

                return ret;
            }

        }
    }

    abstract class DescendingIterator<K,V>(val m:BTreeMap<K,V>){

        protected val descLeafIter = m.descendingLeafIterator(null)
        protected var currentPos = -1
        protected var currentLeaf:Node? = null
        protected var lastReturnedKey: K? = null

        init{
            advanceNode();
        }


        fun hasNext():Boolean = currentLeaf!=null

        fun remove() {
            m.remove(lastReturnedKey ?: throw IllegalStateException())
            this.lastReturnedKey = null
        }

        protected fun advanceNode(){
            if(descLeafIter.hasNext().not()) {
                currentLeaf = null
                currentPos = -1
                return
            }

            var node:Node
            do{
                node = descLeafIter.next()
            }while(node.isEmpty(m.keySerializer) && descLeafIter.hasNext())

            if(node.isEmpty(m.keySerializer)){
                //reached end
                currentPos = -1
                currentLeaf = null
            }else{
                currentLeaf = node
                currentPos = m.keySerializer.valueArraySize(node.keys)-2+node.intLastKeyTwice()+node.intRightEdge()
            }
        }


        protected fun advance(){
            val currentLeaf:Node = currentLeaf?:return
            lastReturnedKey = m.keySerializer.valueArrayGet(currentLeaf.keys, currentPos) as K
            currentPos--

            if(currentPos< 1-currentLeaf.intLeftEdge()){
                advanceNode()
            }
        }
    }


    abstract class DescendingBoundIterator<K,V>(
            val m:BTreeMap<K,V>,
            val lo:K?,
            val loInclusive:Boolean,
            val hi:K?,
            val hiInclusive: Boolean
        ){

        protected val descLeafIter = m.descendingLeafIterator(hi)
        protected var currentPos = -1
        protected var currentLeaf:Node? = null
        protected var lastReturnedKey: K? = null
        protected val loC = if(loInclusive) 0 else -1


        init{
            advanceNode();
        }


        fun hasNext():Boolean = currentLeaf!=null

        fun remove() {
            m.remove(lastReturnedKey ?: throw IllegalStateException())
            this.lastReturnedKey = null
        }

        protected fun advanceNode(){
            val iter = descLeafIter
            val key = hi
            val inclusive = hiInclusive

            while(iter.hasNext()){
                val node = iter.next()
                if(node.isEmpty(m.keySerializer))
                    continue

                var pos=-1;
                val maxPos = m.keySerializer.valueArraySize(node.keys) - 2 + node.intLastKeyTwice() + node.intRightEdge()
                if(key==null) {
                    pos = maxPos
                }else{
                    pos = m.keySerializer.valueArraySearch(node.keys, key, m.comparator)
                    if(pos<0)
                        pos=-pos-2
                    else if(!inclusive)
                        pos--

                    if(pos<1-node.intLeftEdge())
                        continue
                    pos = Math.min(pos, maxPos)
                }

                currentLeaf = node
                currentPos = pos
                checkLoBound()
                return
            }
            currentLeaf = null

        }


        protected fun advance(){
            val currentLeaf:Node = currentLeaf?:return
            lastReturnedKey = m.keySerializer.valueArrayGet(currentLeaf.keys, currentPos) as K
            currentPos--

            if(currentPos< 1-currentLeaf.intLeftEdge()){
                advanceNode()
            }
            checkLoBound()
        }

        protected fun checkLoBound(){
            if(lo==null)
                return;
            val leaf = currentLeaf
                    ?:return
            val currKey = m.keySerializer.valueArrayGet(leaf.keys, currentPos)
            if (m.comparator.compare(lo, currKey) > loC) {
                //reached end, so cancel iteration
                currentLeaf = null
                currentPos = -1;
            }
        }
    }


    override fun descendingEntryIterator(): MutableIterator<MutableMap.MutableEntry<K, V?>> {
        return object : DescendingIterator<K, V>(this), MutableIterator<MutableMap.MutableEntry<K, V?>> {
            override fun next(): MutableMap.MutableEntry<K, V?> {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val key = keySerializer.valueArrayGet(leaf.keys, currentPos)
                val value = valueSerializer.valueArrayGet(leaf.values, currentPos - 1 + leaf.intLeftEdge())
                advance()
                return btreeEntry(key, value)
            }
        }
    }


    override fun descendingEntryIterator(lo:K?,loInclusive:Boolean,hi:K?,hiInclusive:Boolean): MutableIterator<MutableMap.MutableEntry<K, V?>> {
        return object : DescendingBoundIterator<K, V>(this, lo, loInclusive, hi, hiInclusive), MutableIterator<MutableMap.MutableEntry<K, V?>> {
            override fun next(): MutableMap.MutableEntry<K, V?> {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val key = keySerializer.valueArrayGet(leaf.keys, currentPos)
                val value = valueSerializer.valueArrayGet(leaf.values, currentPos - 1 + leaf.intLeftEdge())
                advance()
                return btreeEntry(key, value)
            }
        }
    }

    override fun descendingKeyIterator(): MutableIterator<K> {
        return object : DescendingIterator<K, V>(this), MutableIterator<K> {
            override fun next(): K {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val key = keySerializer.valueArrayGet(leaf.keys, currentPos)
                advance()
                return key
            }
        }
    }

    override fun descendingKeyIterator(lo:K?,loInclusive:Boolean,hi:K?,hiInclusive:Boolean): MutableIterator<K> {
        return object : DescendingBoundIterator<K, V>(this, lo, loInclusive, hi, hiInclusive), MutableIterator<K> {
            override fun next(): K {
                val leaf = currentLeaf
                        ?: throw NoSuchElementException()
                val key = keySerializer.valueArrayGet(leaf.keys, currentPos)
                advance()
                return key
            }
        }
    }

    override fun descendingValueIterator(): MutableIterator<V> {
        return object : DescendingIterator<K, V>(this), MutableIterator<V> {
            override fun next(): V {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val value = valueSerializer.valueArrayGet(leaf.values, currentPos - 1 + leaf.intLeftEdge())
                advance()
                return value
            }
        }
    }

    override fun descendingValueIterator(lo:K?,loInclusive:Boolean,hi:K?,hiInclusive:Boolean): MutableIterator<V> {
        return object : DescendingBoundIterator<K, V>(this, lo, loInclusive, hi, hiInclusive), MutableIterator<V> {
            override fun next(): V {
                val leaf = currentLeaf ?: throw NoSuchElementException()
                val value = valueSerializer.valueArrayGet(leaf.values, currentPos - 1 + leaf.intLeftEdge())
                advance()
                return value
            }
        }
    }


    protected fun btreeEntry(key: K, valueOrig: V): MutableMap.MutableEntry<K, V?> {
        return object : MutableMap.MutableEntry<K, V?> {
            override val key: K
                get() = key

            override val value: V?
                get() = valueCached ?: this@BTreeMap.get(key)

            /** cached value, if null get value from map */
            private var valueCached: V? = valueOrig;

            override fun hashCode(): Int {
                return keySerializer.hashCode(this.key!!, 0) xor valueSerializer.hashCode(this.value!!, 0)
            }

            override fun setValue(newValue: V?): V? {
                valueCached = null;
                return put(key, newValue)
            }


            override fun equals(other: Any?): Boolean {
                if (other !is Map.Entry<*, *>)
                    return false
                val okey = other.key ?: return false
                val ovalue = other.value ?: return false
                try {
                    return comparator.compare(key, okey as K)==0
                            && valueSerializer.equals(this.value!!, ovalue as V)
                } catch(e: ClassCastException) {
                    return false
                }
            }

            override fun toString(): String {
                return "MapEntry[${key}=${value}]"
            }

        }
    }

    override fun hashCode(): Int {
        var h = 0
        val i = entries.iterator()
        while (i.hasNext())
            h += i.next().hashCode()
        return h
    }

    override fun equals(other: Any?): Boolean {
        if (other === this)
            return true

        if (other !is java.util.Map<*, *>)
            return false

        if (other.size() != size)
            return false

        try {
            val i = entries.iterator()
            while (i.hasNext()) {
                val e = i.next()
                val key = e.key
                val value = e.value
                if (value == null) {
                    if (!(other.get(key) == null && other.containsKey(key)))
                        return false
                } else {
                    if (value != other.get(key))
                        return false
                }
            }
        } catch (unused: ClassCastException) {
            return false
        } catch (unused: NullPointerException) {
            return false
        }


        return true
    }


    override fun isClosed(): Boolean {
        return store.isClosed()
    }


    override fun forEach(action: BiConsumer<in K, in V>?) {
        if (action == null)
            throw NullPointerException()
        var node = getNode(leftEdges.first)
        while (true) {
            val limit = keySerializer.valueArraySize(node.keys) - 1 + node.intRightEdge() + node.intLastKeyTwice()
            for (i in 1 - node.intLeftEdge() until limit) {
                val key = keySerializer.valueArrayGet(node.keys, i)
                val value = valueSerializer.valueArrayGet(node.values, i - 1 + node.intLeftEdge())
                action.accept(key, value)
            }

            if (node.isRightEdge)
                return
            node = getNode(node.link)
        }
    }

    override fun forEachKey(procedure: (K) -> Unit) {
        if (procedure == null)
            throw NullPointerException()
        var node = getNode(leftEdges.first)
        while (true) {

            val limit = keySerializer.valueArraySize(node.keys) - 1 + node.intRightEdge() + node.intLastKeyTwice()
            for (i in 1 - node.intLeftEdge() until limit) {
                val key = keySerializer.valueArrayGet(node.keys, i)
                procedure(key)
            }

            if (node.isRightEdge)
                return
            node = getNode(node.link)
        }
    }

    override fun forEachValue(procedure: (V) -> Unit) {
        if (procedure == null)
            throw NullPointerException()
        var node = getNode(leftEdges.first)
        while (true) {
            val limit = keySerializer.valueArraySize(node.keys) - 1 + node.intRightEdge() + node.intLastKeyTwice()
            for (i in 1 - node.intLeftEdge() until limit) {
                val value = valueSerializer.valueArrayGet(node.values, i - 1 + node.intLeftEdge())
                procedure(value)
            }

            if (node.isRightEdge)
                return
            node = getNode(node.link)
        }

    }


    @Throws(ObjectStreamException::class)
    private fun writeReplace(): Any {
        val ret = ConcurrentSkipListMap<Any?, Any?>()
        forEach { k, v ->
            ret.put(k, v)
        }
        return ret
    }


    override fun subMap(fromKey: K?,
                        fromInclusive: Boolean,
                        toKey: K?,
                        toInclusive: Boolean): ConcurrentNavigableMap<K, V> {
        if (fromKey == null || toKey == null)
            throw NullPointerException()
        return SubMap(this, fromKey, fromInclusive, toKey, toInclusive)
    }

    override fun headMap(toKey: K?,
                         inclusive: Boolean): ConcurrentNavigableMap<K, V> {
        if (toKey == null)
            throw NullPointerException()
        return SubMap(this, null, false, toKey, inclusive)
    }

    override fun tailMap(fromKey: K?,
                         inclusive: Boolean): ConcurrentNavigableMap<K, V> {
        if (fromKey == null)
            throw NullPointerException()
        return SubMap(this, fromKey, inclusive, null, false)
    }

    override fun subMap(fromKey: K, toKey: K): ConcurrentNavigableMap<K, V> {
        return subMap(fromKey, true, toKey, false)
    }

    override fun headMap(toKey: K): ConcurrentNavigableMap<K, V> {
        return headMap(toKey, false)
    }

    override fun tailMap(fromKey: K): ConcurrentNavigableMap<K, V> {
        return tailMap(fromKey, true)
    }


    override fun comparator(): Comparator<in K>? {
        return comparator
    }

    override fun firstEntry(): MutableMap.MutableEntry<K, V?>? {
        //get first node
        var node = getNode(leftEdges.first)
        //until empty, follow link
        while (node.isEmpty(keySerializer)) {
            if (node.isRightEdge)
                return null;
            node = getNode(node.link)
        }
        val key = keySerializer.valueArrayGet(node.keys, 1 - node.intLeftEdge())
        val value = valueSerializer.valueArrayGet(node.values, 0)

        //TODO SimpleImmutableEntry etc does not use key/valueSerializer hash code, this is at multiple places
        return AbstractMap.SimpleImmutableEntry(key as K, value as V)
    }

    override fun lastEntry(): MutableMap.MutableEntry<K, V?>? {
        val iter = descendingLeafIterator(null)
        while(iter.hasNext()){
            val node = iter.next()
            if(node.isEmpty(keySerializer)) {
                continue
            }
            val key = keySerializer.valueArrayGet(
                    node.keys,
                    keySerializer.valueArraySize(node.keys) - 2 + node.intLastKeyTwice() + node.intRightEdge()
            )
            val value = valueSerializer.valueArrayGet(
                    node.values,
                    valueSerializer.valueArraySize(node.values) - 1
            )

            return AbstractMap.SimpleImmutableEntry(key, value)

        }
        return null
    }

    override fun firstKey2(): K? {
        //get first node
        var node = getNode(leftEdges.first)
        //until empty, follow link
        while (node.isEmpty(keySerializer)) {
            if (node.isRightEdge)
                return null;
            node = getNode(node.link)
        }
        return keySerializer.valueArrayGet(node.keys, 1 - node.intLeftEdge())
    }

    override fun lastKey2(): K? {
        val iter = descendingLeafIterator(null)
        while(iter.hasNext()){
            val node = iter.next()
            if(node.isEmpty(keySerializer)) {
                continue
            }
            return keySerializer.valueArrayGet(
                    node.keys,
                    keySerializer.valueArraySize(node.keys) - 2 + node.intLastKeyTwice() + node.intRightEdge()
            )
        }
        return null
    }

    override fun firstKey(): K {
        return firstKey2()?:
                throw NoSuchElementException()
    }

    override fun lastKey(): K {
        return lastKey2()?:
                throw NoSuchElementException()
    }

    override fun pollFirstEntry(): MutableMap.MutableEntry<K, V?>? {
        while (true) {
            val e = firstEntry()
                    ?: return null
            if(remove(e.key, e.value))
                return AbstractMap.SimpleImmutableEntry(e.key, e.value);
        }
    }

    override fun pollLastEntry(): MutableMap.MutableEntry<K, V?>? {
        while (true) {
            val e = lastEntry()
                    ?: return null
            if(remove(e.key, e.value))
                return AbstractMap.SimpleImmutableEntry(e.key, e.value);
        }
    }


    override fun findHigher(key: K?, inclusive: Boolean): MutableMap.MutableEntry<K, V>? {
        var current = rootRecid
        var A = getNode(current)

        //dive into bottom
        while (A.isDir) {
            current = findChild(keySerializer, A, comparator, key)
            A = getNode(current)
        }


        //follow link until necessary
        while(true){
            var pos = keySerializer.valueArraySearch(A.keys, key, comparator)
            if(!inclusive && pos>=1-A.intLeftEdge())
                pos++

            if(pos<0)
                pos = -pos-1

            if(pos==0 && !A.isLeftEdge)
                pos++

            //check if is last key
            if(pos< keySerializer.valueArraySize(A.keys)-1+A.intLastKeyTwice()+A.intRightEdge()){
                val key = keySerializer.valueArrayGet(A.keys, pos)
                val value = leafGet(A, pos, keySerializer, valueSerializer)
                return AbstractMap.SimpleImmutableEntry(key, value as V)
            }

            if(A.isRightEdge){
                //reached end, cancel iteration
                return null
            }
            //load next node
            A = getNode(A.link)
        }
    }

    override fun findLower(key: K?, inclusive: Boolean): MutableMap.MutableEntry<K, V>? {
        val iter = descendingLeafIterator(key)
        while(iter.hasNext()){
            val node = iter.next()
            if(node.isEmpty(keySerializer))
                continue

            var pos = keySerializer.valueArraySearch(node.keys, key, comparator)

            if(pos==-1)
                continue
            if(pos==0 && !inclusive)
                continue

            if(pos>=1-node.intLeftEdge() && !inclusive)
                pos--

            if(pos>=keySerializer.valueArraySize(node.keys)-1+node.intRightEdge()+node.intLastKeyTwice())
                pos--

            if(pos>=1-node.intLeftEdge()){
                //node was found
                val key = keySerializer.valueArrayGet(node.keys, pos)
                val value = valueSerializer.valueArrayGet(node.values, pos - 1 + node.intLeftEdge())
                return AbstractMap.SimpleImmutableEntry(key, value)
            }

            if(inclusive && pos == 1-node.intLeftEdge()){
                pos = 1-node.intLeftEdge()
                val key = keySerializer.valueArrayGet(node.keys, pos)
                val value = valueSerializer.valueArrayGet(node.values, pos-1+node.intLeftEdge())
                return AbstractMap.SimpleImmutableEntry(key, value)
            }

            if(pos<0){
                pos = - pos - 2
                if(pos>=keySerializer.valueArraySize(node.keys)-1+node.intRightEdge()+node.intLastKeyTwice())
                    pos--

                if(pos<1-node.intLeftEdge())
                    continue

                val key = keySerializer.valueArrayGet(node.keys, pos)
                val value = valueSerializer.valueArrayGet(node.values, pos - 1 + node.intLeftEdge())
                return AbstractMap.SimpleImmutableEntry(key, value)
            }
        }
        return null
    }

    override fun findHigherKey(key: K?, inclusive: Boolean): K? {
        var current = rootRecid
        var A = getNode(current)

        //dive into bottom
        while (A.isDir) {
            current = findChild(keySerializer, A, comparator, key)
            A = getNode(current)
        }


        //follow link until necessary
        while(true){
            var pos = keySerializer.valueArraySearch(A.keys, key, comparator)
            if(!inclusive && pos>=1-A.intLeftEdge())
                pos++

            if(pos<0)
                pos = -pos-1

            if(pos==0 && !A.isLeftEdge)
                pos++

            //check if is last key
            if(pos< keySerializer.valueArraySize(A.keys)-1+A.intLastKeyTwice()+A.intRightEdge()){
                return keySerializer.valueArrayGet(A.keys, pos)
            }

            if(A.isRightEdge){
                //reached end, cancel iteration
                return null
            }
            //load next node
            A = getNode(A.link)
        }
    }

    override fun findLowerKey(key: K?, inclusive: Boolean): K? {
        val iter = descendingLeafIterator(key)
        while(iter.hasNext()){
            val node = iter.next()
            if(node.isEmpty(keySerializer))
                continue

            var pos = keySerializer.valueArraySearch(node.keys, key, comparator)

            if(pos==-1)
                continue
            if(pos==0 && !inclusive)
                continue

            if(pos>=1-node.intLeftEdge() && !inclusive)
                pos--

            if(pos>=keySerializer.valueArraySize(node.keys)-1+node.intRightEdge()+node.intLastKeyTwice())
                pos--

            if(pos>=1-node.intLeftEdge()){
                //node was found
                return keySerializer.valueArrayGet(node.keys, pos)
            }

            if(inclusive && pos == 1-node.intLeftEdge()){
                pos = 1-node.intLeftEdge()
                return keySerializer.valueArrayGet(node.keys, pos)
            }

            if(pos<0){
                pos = - pos - 2
                if(pos>=keySerializer.valueArraySize(node.keys)-1+node.intRightEdge()+node.intLastKeyTwice())
                    pos--

                if(pos<1-node.intLeftEdge())
                    continue

                return keySerializer.valueArrayGet(node.keys, pos)
            }
        }
        return null
    }



    override fun lowerEntry(key: K?): MutableMap.MutableEntry<K, V>? {
        if (key == null) throw NullPointerException()
        return findLower(key, false)
    }

    override fun lowerKey(key: K): K? {
        return findLowerKey(key, false)
    }

    override fun floorEntry(key: K?): MutableMap.MutableEntry<K, V>? {
        if (key == null) throw NullPointerException()
        return findLower(key, true)
    }

    override fun floorKey(key: K): K? {
        return findLowerKey(key, true)
    }

    override fun ceilingEntry(key: K?): MutableMap.MutableEntry<K, V>? {
        if (key == null) throw NullPointerException()
        return findHigher(key, true)
    }


    override fun ceilingKey(key: K?): K? {
        if (key == null) throw NullPointerException()
        return findHigherKey(key, true)
    }

    override fun higherEntry(key: K?): MutableMap.MutableEntry<K, V>? {
        if (key == null) throw NullPointerException()
        return findHigher(key, false)
    }

    override fun higherKey(key: K?): K? {
        if (key == null) throw NullPointerException()
        return findHigherKey(key, false)
    }

}