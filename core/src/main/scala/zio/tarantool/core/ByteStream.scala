package zio.tarantool.core

import scodec.bits.ByteVector
import zio.{Chunk, ChunkBuilder, ZRef}
import zio.stream.ZTransducer
import zio.tarantool.protocol.MessagePackPacket
import zio.tarantool.codec.MessagePackPacketCodec
import zio.tarantool.protocol.Implicits.{RichByteVector, RichMessagePack}

object ByteStream {
  private val MessageSizeLength = 5

  val decoder: ZTransducer[Any, Nothing, Byte, MessagePackPacket] =
    ZTransducer {
      ZRef.makeManaged[State](State.empty).map { stateRef =>
        {
          case None =>
            stateRef.modify {
              case State(length, data) if length != 0 && data.length == length =>
                val vector = ByteVector.view(data.toArray)
                (
                  Chunk.single(MessagePackPacketCodec.decodeValue(vector.toBitVector).require),
                  State(0, Chunk.empty)
                )
              case state => (Chunk.empty, state)
            }
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
        // length part was not read
        if (state.dataChunk.length >= MessageSizeLength) {
          // dataChunk length is enough to decode length part
          val (lengthChunk, dataRemainderChunk) = state.dataChunk.splitAt(MessageSizeLength)
          val vector = ByteVector.view(lengthChunk.toArray)
          val length = vector.decodeUnsafe().toNumber.toInt
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
          val vector = ByteVector(dataChunk.toArray)
          val packet = MessagePackPacketCodec.decodeValue(vector.toBitVector).require
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

  final case class State(
    length: Int,
    dataChunk: Chunk[Byte]
  )

  object State {
    def empty = new State(0, Chunk.empty)
  }
}
