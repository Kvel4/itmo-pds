package dijkstra

import dijkstra.messages.*
import dijkstra.system.environment.Environment

class ProcessImpl(private val environment: Environment) : Process {
    private var distance = Long.MAX_VALUE
    private var messagesCounter = 0
    private var parent = -1

    override fun onMessage(srcId: Int, message: Message) {
        if (isPassive()) {
            parent = srcId
            messagesCounter++
            environment.send(srcId, AddChildMessage)
        }

        when (message) {
            is DistanceMessage -> {
                environment.send(srcId, AcknowledgmentMessage)

                if (message.distance < distance) {
                    distance = message.distance
                    for (neighbour in environment.neighbours) {
                        messagesCounter++
                        environment.send(neighbour.key, DistanceMessage(distance + neighbour.value))
                    }
                }
            }
            is AddChildMessage -> {
                messagesCounter++
                environment.send(srcId, AcknowledgmentMessage)
            }
            is AcknowledgmentMessage -> messagesCounter--
            is DeleteChildMessage -> messagesCounter--
        }

        if (isPassive()) {
            if (parent == -1) {
                environment.finishExecution()
            } else {
                environment.send(parent, DeleteChildMessage)
            }
        }
    }

    override fun getDistance(): Long? {
        if (distance == Long.MAX_VALUE) return null
        return distance
    }

    override fun startComputation() {
        distance = 0

        if (environment.neighbours.isEmpty()) environment.finishExecution()

        for (neighbour in environment.neighbours) {
            messagesCounter++
            environment.send(neighbour.key, DistanceMessage(distance + neighbour.value))
        }
    }

    private fun isPassive() = messagesCounter == 0
}