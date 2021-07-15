package zio.tarantool.internal

import org.msgpack.core.MessagePack
import zio.{Chunk, ChunkBuilder, ZRef}
import zio.stream.ZTransducer
import zio.tarantool.protocol.MessagePackPacket
import zio.tarantool.codec.MessagePackPacketSerDe

private[tarantool] object ByteStream {
  private val MessageSizeLength = 5

  val decoder: ZTransducer[Any, Nothing, Byte, MessagePackPacket] =
    ZTransducer {
      ZRef.makeManaged[State](State.empty).map { stateRef =>
        {
          // no new data was read
          case None =>
            stateRef.modify {
              // length part was read and is ready to be decoded
              case State(length, data) if length != 0 && data.length == length =>
                (
                  Chunk.single(MessagePackPacketSerDe.deserialize(data.toArray)),
                  State(0, Chunk.empty)
                )
              case state => (Chunk.empty, state)
            }
          // read new data and try to decode it
          case Some(bytes) =>
            stateRef.modify { oldState =>
              decodeByteStream(oldState.copy(dataChunk = oldState.dataChunk ++ bytes))
            }
        }
      }
    }

  /**
   * @return - decoded packets and new state
   */
  private def decodeByteStream(state: State): (Chunk[MessagePackPacket], State) = {
    def go(state: State, acc: ChunkBuilder[MessagePackPacket]): State =
      if (state.length == 0) {
        println("HERE!!!!")
        // length part was not read
        if (state.dataChunk.length >= MessageSizeLength) {
          // dataChunk length is enough to decode length part
          val (lengthChunk, dataRemainderChunk) = state.dataChunk.splitAt(MessageSizeLength)
          val unpacker = MessagePack.newDefaultUnpacker(lengthChunk.toArray)
          val length = unpacker.unpackInt()
          unpacker.close()
          go(State(length, dataRemainderChunk), acc)
        } else {
          // dataChunk length is not enough to decode length part
          state
        }
      } else {
        // length part was read
        if (state.dataChunk.length >= state.length) {
          // dataChunk length is enough to decode packet
          val (dataChunk, remainderChunk) = state.dataChunk.splitAt(state.length)
          val packet = MessagePackPacketSerDe.deserialize(dataChunk.toArray)
          acc += packet
          go(State(0, remainderChunk), acc)
        } else {
          // dataChunk length is not enough to decode packet
          state
        }
      }

    val data = ChunkBuilder.make[MessagePackPacket]()
    val newState = go(state, data)
    (data.result(), newState)
  }

  /**
   * @param length -- already read data length
   * @param dataChunk -- actual read data
   */
  final case class State(
    length: Int,
    dataChunk: Chunk[Byte]
  )

  object State {
    // Initial state for the new connection
    def empty = new State(0, Chunk.empty)
  }
}
