package dijkstra.messages

sealed class Message

object AddChildMessage : Message()
object DeleteChildMessage : Message()
object AcknowledgmentMessage : Message()

data class DistanceMessage(val distance: Long) : Message()