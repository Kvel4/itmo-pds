import system.MergerEnvironment
import java.util.PriorityQueue

class MergerImpl<T : Comparable<T>>(
    private val mergerEnvironment: MergerEnvironment<T>,
    prevStepBatches: Map<Int, List<T>>?
) : Merger<T> {
    private val dataHoldersCount = mergerEnvironment.dataHoldersCount
    private val priorityQueue = PriorityQueue<Pair<Int, ArrayDeque<T>>>( compareBy { it.second.first() } )

    init {
        (0 until dataHoldersCount).forEach { i ->
            prevStepBatches?.get(i)?.let { keys ->
                priorityQueue.add(Pair(i, ArrayDeque(keys)))
            } ?: requestAndAdd(i)
        }
    }

    override fun mergeStep(): T? = priorityQueue.poll()?.let { top ->
        val deque = top.second
        deque.removeFirst().also {
            deque.firstOrNull()?.let {
                priorityQueue.add(top)
            } ?: requestAndAdd(top.first)
        }
    }

    override fun getRemainingBatches(): Map<Int, List<T>> = priorityQueue.toMap()

    private fun requestAndAdd(i: Int) {
        mergerEnvironment.requestBatch(i).let {
            if (it.isNotEmpty()) {
                priorityQueue.add(Pair(i, ArrayDeque(it)))
            }
        }
    }
}