class ConsistentHashImpl<K> : ConsistentHash<K> {
    private val vnodes = mutableListOf<Int>()
    private val hashToShard = mutableMapOf<Int, Shard>()

    override fun getShardByKey(key: K): Shard {
        return hashToShard[upperBound(key.hashCode())]!!
    }

    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        val hashes = vnodeHashes.toList()

        if (vnodes.isEmpty()) {
            hashes.sorted().forEach {
                hashToShard[it] = newShard
                vnodes.add(it)
            }
            return emptyMap()
        }

        val resultMap = computeShardRanges(hashes)

        hashes.forEach {
            vnodes.add(lowerBoundInd(it) + 1, it)
            hashToShard[it] = newShard
        }

        return resultMap
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        val hashes = vnodes.filter { hashToShard[it] == shard }
        vnodes.removeAll { hashToShard[it] == shard }

        return computeShardRanges(hashes)
    }

    private fun computeShardRanges(hashes: List<Int>): MutableMap<Shard, Set<HashRange>> {
        val resultMap = mutableMapOf<Shard, Set<HashRange>>()
        val maxRanges = mutableMapOf<Int, Int>()

        hashes.forEach {
            val l = lowerBound(it)
            maxRanges.merge(l, it) { oldR, newR ->
                val oldRange = computeRange(l, oldR)
                val newRange = computeRange(l, newR)
                if (newRange > oldRange) newR else oldR
            }
        }

        maxRanges.forEach {
            val (l, r) = it
            val u = upperBound(r)
            resultMap.merge(hashToShard[u]!!, setOf(HashRange(l + 1, r))) { s1, s2 -> s1 + s2 }
        }

        return resultMap
    }

    private fun lowerBound(x: Int): Int {
        val i = lowerBoundInd(x)
        if (i == -1) return vnodes.last()
        return vnodes[i]
    }

    private fun upperBound(x: Int): Int {
        val i = upperBoundInd(x)
        if (i == vnodes.size) return vnodes.first()
        return vnodes[i]
    }
    private fun upperBoundInd(x: Int): Int {
        var l = -1
        var r = vnodes.size
        while (r - l > 1) {
            val mid =  l + (r - l) / 2
            if (vnodes[mid] >= x) {
                r = mid
            } else {
                l = mid
            }
        }
        return r
    }

    private fun lowerBoundInd(x: Int): Int {
        var l = -1
        var r = vnodes.size
        while (r - l > 1) {
            val mid =  l + (r - l) / 2
            if (vnodes[mid] < x) {
                l = mid
            } else {
                r = mid
            }
        }
        return l
    }

    private fun computeRange(l: Int, r: Int) =
        if (r < l) {
            Int.MAX_VALUE.toLong() - l + r - Int.MIN_VALUE
        } else {
            r.toLong() - l
        }
 }
