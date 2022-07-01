package raft

import raft.Message.*
import java.lang.Integer.min

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author Monakhov Daniil
 */
class ProcessImpl(private val env: Environment) : Process {
    private val storage = env.storage
    private val machine = env.machine

    // All servers
    private val commandQueue = ArrayDeque<Command>()
    private var role = Role.FOLLOWER
    private var commitIndex = 0
    private var lastApplied = 0
    private var leaderId: Int? = null

    // Leader only
    private var appendEntryInProgress = BooleanArray(env.nProcesses + 1)
    private var nextIndex = emptyArray<Int>()
    private var matchIndex = emptyArray<Int>()

    // Candidate only
    private var votesCnt = 0


    init {
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    override fun onTimeout() {
        when (role) {
            Role.FOLLOWER, Role.CANDIDATE -> startElection()
            Role.LEADER -> {
                sendHeartbeat((1..env.nProcesses).toList())
                env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
            }
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        var (term, votedFor) = storage.readPersistentState()

        if (message.term > term) {
            role = Role.FOLLOWER
            storage.writePersistentState(PersistentState(message.term))
            env.startTimeout(Timeout.ELECTION_TIMEOUT)
            term = message.term
            votedFor = null
            votesCnt = 0
        }

        if (message.isLeaderMessage()) {
            leaderId = srcId
        }

        when (message) {
            is AppendEntryRpc -> {
                if (message.term < term) {
                    env.send(srcId, AppendEntryResult(term, null))
                    return
                }

                role = Role.FOLLOWER
                env.startTimeout(Timeout.ELECTION_TIMEOUT)
                processCommandQueue()


                if (message.prevLogId != storage.readPrevLogId(message.prevLogId.index)) {
                    env.send(srcId, AppendEntryResult(term, null))
                    return
                }

                message.entry?.let { storage.appendLogEntry(it) }

                if (message.leaderCommit > commitIndex) {
                    commitIndex = min(message.leaderCommit, storage.readLastLogId().index)
                    applyCommand()
                }

                env.send(srcId, AppendEntryResult(term, message.entry?.id?.index ?: message.prevLogId.index))
            }
            is RequestVoteRpc -> {
                if (message.term < term) {
                    env.send(srcId, RequestVoteResult(term, false))
                    return
                }

                if (votedFor == srcId) {
                    env.send(srcId, RequestVoteResult(term, true))
                    return
                }

                if (votedFor == null && message.lastLogId >= storage.readLastLogId()) {
                    storage.writePersistentState(PersistentState(term, srcId))
                    env.send(srcId, RequestVoteResult(term, true))
                    env.startTimeout(Timeout.ELECTION_TIMEOUT)
                    return
                }

                env.send(srcId, RequestVoteResult(term, false))
            }
            is ClientCommandRpc -> {
                onClientCommand(message.command)
            }
            is AppendEntryResult -> {
                if (message.term < term || role != Role.LEADER) return

                appendEntryInProgress[srcId] = false

                if (message.lastIndex == null) {
                    nextIndex[srcId]--
                    sendAppendRpc(srcId, term, true)
                    return
                }

                nextIndex[srcId] = message.lastIndex + 1
                matchIndex[srcId] = message.lastIndex

                var newCommitIndex = commitIndex
                while (++newCommitIndex <= storage.readLastLogId().index) {
                    if (storage.readLog(newCommitIndex)!!.id.term != term) continue

                    var replicatedCnt = 0
                    for (matchInd in matchIndex) {
                        if (matchInd >= newCommitIndex) replicatedCnt++
                    }

                    if (replicatedCnt < env.nProcesses / 2) break

                    commitIndex = newCommitIndex
                    applyCommand()
                }

            }
            is RequestVoteResult -> {
                if (message.term < term || role != Role.CANDIDATE) return

                if (message.voteGranted) votesCnt++

                if (votesCnt > env.nProcesses / 2) {
                    val lastId = storage.readLastLogId()
                    role = Role.LEADER

                    nextIndex = Array(env.nProcesses + 1) { lastId.index + 1 }
                    matchIndex = Array(env.nProcesses + 1) { 0 }
                    appendEntryInProgress = BooleanArray(env.nProcesses + 1)

                    processCommandQueue()
                    sendHeartbeat((1..env.nProcesses).toList())
                    env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
                }
            }
            is ClientCommandResult -> {
                env.onClientCommandResult(message.result)
                processCommandQueue()
            }
        }
    }

    override fun onClientCommand(command: Command) {
        commandQueue.add(command)
        processCommandQueue()
    }

    private fun startElection() {
        val term = storage.readPersistentState().currentTerm + 1
        role = Role.CANDIDATE
        leaderId = null
        votesCnt = 1

        storage.writePersistentState(PersistentState(term, env.processId))
        env.startTimeout(Timeout.ELECTION_TIMEOUT)

        sendAll(RequestVoteRpc(term, storage.readLastLogId()))
    }

    private fun processCommandQueue() {
        val term = storage.readPersistentState().currentTerm
        while (commandQueue.isNotEmpty()) {
            if (role == Role.LEADER) {
                commitCommand(commandQueue.removeFirst())
            } else {
                leaderId?.let {
                    env.send(it, ClientCommandRpc(term, commandQueue.removeFirst()))
                } ?: break
            }
        }
    }

    private fun commitCommand(command: Command) {
        val lastLog = storage.readLastLogId()

        storage.appendLogEntry(
            LogEntry(
                LogId(lastLog.index + 1, storage.readPersistentState().currentTerm), command
            )
        )
        sendHeartbeat((1..env.nProcesses).filter { !appendEntryInProgress[it] }, true)
    }

    private fun applyCommand() {
        val term = storage.readPersistentState().currentTerm
        var command: Command? = null
        var result: CommandResult? = null

        while (commitIndex > lastApplied) {
            command = storage.readLog(++lastApplied)!!.command
            result = machine.apply(command)
        }

        if (role == Role.LEADER) {
            if (command!!.processId == env.processId) {
                env.onClientCommandResult(result!!)
            } else {
                env.send(command.processId, ClientCommandResult(term, result!!))
            }
        }

    }

    private fun sendHeartbeat(processes: Collection<Int>, progressStatus: Boolean = false) {
        val state = storage.readPersistentState()
        processes.filter { it != env.processId }.forEach {
            sendAppendRpc(it, state.currentTerm, progressStatus)
        }
    }

    private fun sendAppendRpc(id: Int, term: Int, progressStatus: Boolean) {
        appendEntryInProgress[id] = progressStatus
        env.send(
            id, AppendEntryRpc(
                term,
                storage.readPrevLogId(nextIndex[id] - 1),
                commitIndex,
                storage.readLog(nextIndex[id])
            )
        )
    }

    private fun sendAll(message: Message) {
        (1..env.nProcesses).filter { it != env.processId }.forEach {
            env.send(it, message)
        }
    }

    private fun Message.isLeaderMessage() = this is AppendEntryRpc || this is ClientCommandResult

    private fun Storage.readPrevLogId(id: Int) = readLog(id)?.id ?: START_LOG_ID
}

enum class Role {
    FOLLOWER, CANDIDATE, LEADER
}
