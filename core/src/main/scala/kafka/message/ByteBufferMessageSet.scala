/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.message

import java.nio.ByteBuffer
<<<<<<< HEAD

import kafka.common.LongRef
import kafka.utils.Logging
import org.apache.kafka.common.record._
=======
import java.nio.channels._
import java.io._
import java.util.ArrayDeque

import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Utils
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3

import scala.collection.JavaConverters._

object ByteBufferMessageSet {

  private def create(offsetAssigner: OffsetAssigner,
                     compressionCodec: CompressionCodec,
                     timestampType: TimestampType,
                     messages: Message*): ByteBuffer = {
    if (messages.isEmpty)
      MessageSet.Empty.buffer
<<<<<<< HEAD
    else {
      val buffer = ByteBuffer.allocate(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
      val builder = MemoryRecords.builder(buffer, messages.head.magic, CompressionType.forId(compressionCodec.codec),
        timestampType, offsetAssigner.baseOffset)

      for (message <- messages)
        builder.appendWithOffset(offsetAssigner.nextAbsoluteOffset(), message.asRecord)
=======
    else if (compressionCodec == NoCompressionCodec) {
      val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      for (message <- messages) writeMessage(buffer, message, offsetAssigner.nextAbsoluteOffset())
      buffer.rewind()
      buffer
    } else {
      val magicAndTimestamp = wrapperMessageTimestamp match {
        case Some(ts) => MagicAndTimestamp(messages.head.magic, ts)
        case None => MessageSet.magicAndLargestTimestamp(messages)
      }
      var offset = -1L
      val messageWriter = new MessageWriter(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
      messageWriter.write(codec = compressionCodec, timestamp = magicAndTimestamp.timestamp, timestampType = timestampType, magicValue = magicAndTimestamp.magic) { outputStream =>
        val output = new DataOutputStream(CompressionFactory(compressionCodec, magicAndTimestamp.magic, outputStream))
        try {
          for (message <- messages) {
            offset = offsetAssigner.nextAbsoluteOffset()
            if (message.magic != magicAndTimestamp.magic)
              throw new IllegalArgumentException("Messages in the message set must have same magic value")
            // Use inner offset if magic value is greater than 0
            if (magicAndTimestamp.magic > Message.MagicValue_V0)
              output.writeLong(offsetAssigner.toInnerOffset(offset))
            else
              output.writeLong(offset)
            output.writeInt(message.size)
            output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
          }
        } finally {
          output.close()
        }
      }
      val buffer = ByteBuffer.allocate(messageWriter.size + MessageSet.LogOverhead)
      writeMessage(buffer, messageWriter, offset)
      buffer.rewind()
      buffer
    }
  }

  /** Deep iterator that decompresses the message sets and adjusts timestamp and offset if needed. */
  def deepIterator(wrapperMessageAndOffset: MessageAndOffset): Iterator[MessageAndOffset] = {

    import Message._

    new IteratorTemplate[MessageAndOffset] {

      val MessageAndOffset(wrapperMessage, wrapperMessageOffset) = wrapperMessageAndOffset
      val wrapperMessageTimestampOpt: Option[Long] =
        if (wrapperMessage.magic > MagicValue_V0) Some(wrapperMessage.timestamp) else None
      val wrapperMessageTimestampTypeOpt: Option[TimestampType] =
        if (wrapperMessage.magic > MagicValue_V0) Some(wrapperMessage.timestampType) else None
      if (wrapperMessage.payload == null)
        throw new KafkaException(s"Message payload is null: $wrapperMessage")
      val inputStream = new ByteBufferBackedInputStream(wrapperMessage.payload)
      val compressed = try {
        new DataInputStream(CompressionFactory(wrapperMessage.compressionCodec, wrapperMessage.magic, inputStream))
      } catch {
        case ioe: IOException =>
          throw new InvalidMessageException(s"Failed to instantiate input stream compressed with ${wrapperMessage.compressionCodec}", ioe)
      }
      var lastInnerOffset = -1L

      val messageAndOffsets = if (wrapperMessageAndOffset.message.magic > MagicValue_V0) {
        val innerMessageAndOffsets = new ArrayDeque[MessageAndOffset]()
        try {
          while (true)
            innerMessageAndOffsets.add(readMessageFromStream())
        } catch {
          case eofe: EOFException =>
            compressed.close()
          case ioe: IOException =>
            throw new InvalidMessageException(s"Error while reading message from stream compressed with ${wrapperMessage.compressionCodec}", ioe)
        }
        Some(innerMessageAndOffsets)
      } else None

      private def readMessageFromStream(): MessageAndOffset = {
        val innerOffset = compressed.readLong()
        val recordSize = compressed.readInt()

        if (recordSize < MinMessageOverhead)
          throw new InvalidMessageException(s"Message found with corrupt size `$recordSize` in deep iterator")

        // read the record into an intermediate record buffer (i.e. extra copy needed)
        val bufferArray = new Array[Byte](recordSize)
        compressed.readFully(bufferArray, 0, recordSize)
        val buffer = ByteBuffer.wrap(bufferArray)

        // Override the timestamp if necessary
        val newMessage = new Message(buffer, wrapperMessageTimestampOpt, wrapperMessageTimestampTypeOpt)

        // Inner message and wrapper message must have same magic value
        if (newMessage.magic != wrapperMessage.magic)
          throw new IllegalStateException(s"Compressed message has magic value ${wrapperMessage.magic} " +
            s"but inner message has magic value ${newMessage.magic}")
        lastInnerOffset = innerOffset
        new MessageAndOffset(newMessage, innerOffset)
      }

      override def makeNext(): MessageAndOffset = {
        messageAndOffsets match {
          // Using inner offset and timestamps
          case Some(innerMessageAndOffsets) =>
            innerMessageAndOffsets.pollFirst() match {
              case null => allDone()
              case MessageAndOffset(message, offset) =>
                val relativeOffset = offset - lastInnerOffset
                val absoluteOffset = wrapperMessageOffset + relativeOffset
                new MessageAndOffset(message, absoluteOffset)
            }
          // Not using inner offset and timestamps
          case None =>
            try readMessageFromStream()
            catch {
              case eofe: EOFException =>
                compressed.close()
                allDone()
              case ioe: IOException =>
                throw new KafkaException(ioe)
            }
        }
      }
    }
  }
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3

      builder.build().buffer
    }
  }

}

private object OffsetAssigner {

  def apply(offsetCounter: LongRef, size: Int): OffsetAssigner =
    new OffsetAssigner(offsetCounter.value to offsetCounter.addAndGet(size))

}

private class OffsetAssigner(offsets: Seq[Long]) {
  private var index = 0

  def nextAbsoluteOffset(): Long = {
    val result = offsets(index)
    index += 1
    result
  }

  def baseOffset = offsets.head

  def toInnerOffset(offset: Long): Long = offset - offsets.head

}

/**
 * A sequence of messages stored in a byte buffer
 *
 * There are two ways to create a ByteBufferMessageSet
 *
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
 *
 * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
 *
 *
 * Message format v1 has the following changes:
 * - For non-compressed messages, timestamp and timestamp type attributes have been added. The offsets of
 *   the messages remain absolute offsets.
 * - For compressed messages, timestamp and timestamp type attributes have been added and inner offsets (IO) are used
 *   for inner messages of compressed messages (see offset calculation details below). The timestamp type
 *   attribute is only set in wrapper messages. Inner messages always have CreateTime as the timestamp type in attributes.
 *
 * We set the timestamp in the following way:
 * For non-compressed messages: the timestamp and timestamp type message attributes are set and used.
 * For compressed messages:
 * 1. Wrapper messages' timestamp type attribute is set to the proper value
 * 2. Wrapper messages' timestamp is set to:
 *    - the max timestamp of inner messages if CreateTime is used
 *    - the current server time if wrapper message's timestamp = LogAppendTime.
 *      In this case the wrapper message timestamp is used and all the timestamps of inner messages are ignored.
 * 3. Inner messages' timestamp will be:
 *    - used when wrapper message's timestamp type is CreateTime
 *    - ignored when wrapper message's timestamp type is LogAppendTime
 * 4. Inner messages' timestamp type will always be ignored with one exception: producers must set the inner message
 *    timestamp type to CreateTime, otherwise the messages will be rejected by broker.
 *
 * Absolute offsets are calculated in the following way:
 * Ideally the conversion from relative offset(RO) to absolute offset(AO) should be:
 *
 * AO = AO_Of_Last_Inner_Message + RO
 *
 * However, note that the message sets sent by producers are compressed in a streaming way.
 * And the relative offset of an inner message compared with the last inner message is not known until
 * the last inner message is written.
 * Unfortunately we are not able to change the previously written messages after the last message is written to
 * the message set when stream compression is used.
 *
 * To solve this issue, we use the following solution:
 *
 * 1. When the producer creates a message set, it simply writes all the messages into a compressed message set with
 *    offset 0, 1, ... (inner offset).
 * 2. The broker will set the offset of the wrapper message to the absolute offset of the last message in the
 *    message set.
 * 3. When a consumer sees the message set, it first decompresses the entire message set to find out the inner
 *    offset (IO) of the last inner message. Then it computes RO and AO of previous messages:
 *
 *    RO = IO_of_a_message - IO_of_the_last_message
 *    AO = AO_Of_Last_Inner_Message + RO
 *
 * 4. This solution works for compacted message sets as well.
 *
 */
class ByteBufferMessageSet(val buffer: ByteBuffer) extends MessageSet with Logging {

  private[kafka] def this(compressionCodec: CompressionCodec,
                          offsetCounter: LongRef,
                          timestampType: TimestampType,
                          messages: Message*) {
    this(ByteBufferMessageSet.create(OffsetAssigner(offsetCounter, messages.size), compressionCodec,
      timestampType, messages:_*))
  }

  def this(compressionCodec: CompressionCodec, offsetCounter: LongRef, messages: Message*) {
    this(compressionCodec, offsetCounter, TimestampType.CREATE_TIME, messages:_*)
  }

  def this(compressionCodec: CompressionCodec, offsetSeq: Seq[Long], messages: Message*) {
    this(ByteBufferMessageSet.create(new OffsetAssigner(offsetSeq), compressionCodec,
      TimestampType.CREATE_TIME, messages:_*))
  }

  def this(compressionCodec: CompressionCodec, messages: Message*) {
    this(compressionCodec, new LongRef(0L), messages: _*)
  }

  def this(messages: Message*) {
    this(NoCompressionCodec, messages: _*)
  }

  def getBuffer = buffer

  override def asRecords: MemoryRecords = MemoryRecords.readableRecords(buffer.duplicate())

  /** default iterator that iterates over decompressed messages */
  override def iterator: Iterator[MessageAndOffset] = internalIterator()

  /** iterator over compressed messages without decompressing */
  def shallowIterator: Iterator[MessageAndOffset] = internalIterator(isShallow = true)

  /** When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. **/
  private def internalIterator(isShallow: Boolean = false): Iterator[MessageAndOffset] = {
    if (isShallow)
      asRecords.batches.asScala.iterator.map(MessageAndOffset.fromRecordBatch)
    else
      asRecords.records.asScala.iterator.map(MessageAndOffset.fromRecord)
  }

  /**
   * The total number of bytes in this message set, including any partial trailing messages
   */
  def sizeInBytes: Int = buffer.limit

  /**
   * The total number of bytes in this message set not including any partial, trailing messages
   */
  def validBytes: Int = asRecords.validBytes

  /**
   * Two message sets are equal if their respective byte buffers are equal
   */
  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet =>
        buffer.equals(that.buffer)
      case _ => false
    }
  }

  override def hashCode: Int = buffer.hashCode

}
