package mutex

/**
 * Distributed mutual exclusion implementation. All functions are called from the single main
 * thread.
 *
 * @author Daniil Monakhov //
 */
class ProcessImpl(private val env: Environment) : Process {
    private var inCS = false
    private var wantCS = false
    private val forks =
        Array(env.nProcesses + 1) { i ->
            if (i == 0 || i >= env.processId) {
                Fork.DIRTY
            } else {
                Fork.ABSENT
            }
        }
    private val pendingFork = BooleanArray(env.nProcesses + 1)

    override fun onMessage(srcId: Int, message: Message) {
        message.parse {
            check(srcId != env.processId) {"self message"}

            val type = readEnum<MsgType>()
            when (type) {
                MsgType.REQ -> {
                    if (forks[srcId] == Fork.DIRTY) {
                        forks[srcId] = Fork.ABSENT
                        send(srcId, MsgType.OK)
                        if (wantCS) send(srcId, MsgType.REQ)
                    } else {
                        check(!pendingFork[srcId])
                        check(forks[srcId] != Fork.ABSENT)
                        pendingFork[srcId] = true
                    }
                }
                MsgType.OK -> {
                    forks[srcId] = Fork.CLEAR
                    checkCSEnter()
                }
            }
        }
    }

    private fun checkCSEnter() {
        if (inCS) return
        if (forks.any { it == Fork.ABSENT }) return

        massOperation { i ->
            forks[i] = Fork.CLEAR
        }

        wantCS = false
        inCS = true
        env.locked()
    }

    override fun onLockRequest() {
        check(!wantCS) { "wtf several lock requests" }

        wantCS = true
        checkCSEnter()
        massOperation { i ->
            if (forks[i] == Fork.ABSENT) {
                send(i, MsgType.REQ)
            }
        }
    }

    override fun onUnlockRequest() {
        check(inCS) { "We are not in critical section" }

        env.unlocked()
        inCS = false

        massOperation { i ->
            if (pendingFork[i]) {
                check (i != env.processId)

                pendingFork[i] = false
                forks[i] = Fork.ABSENT

                send(i, MsgType.OK)
            } else {
                forks[i] = Fork.DIRTY
            }
        }
    }

    private fun massOperation(operation: (Int) -> Unit) {
        for (i in 1..env.nProcesses) {
            if (i != env.processId) {
                operation(i)
            }
        }
    }

    private fun send(destId: Int, type: MsgType, builder: MessageBuilder.() -> Unit = {}) {
        env.send(destId) {
            writeEnum(type)
            builder()
        }
    }

    enum class MsgType {
        REQ,
        OK
    }

    enum class Fork {
        CLEAR,
        DIRTY,
        ABSENT
    }
}
