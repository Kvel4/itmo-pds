import system.DataHolderEnvironment
import java.lang.Integer.min

class DataHolderImpl<T : Comparable<T>>(
    private val keys: List<T>,
    private val dataHolderEnvironment: DataHolderEnvironment
) : DataHolder<T> {
    private val batchSize = dataHolderEnvironment.batchSize
    private var position = 0
    private var savedPosition = 0

    override fun checkpoint() {
        savedPosition = position
    }

    override fun rollBack() {
        position = savedPosition
    }

    override fun getBatch(): List<T> = (position + batchSize).let { upper ->
        keys.slice(position until min(upper, keys.size)).also {
            position = upper
        }
    }
}