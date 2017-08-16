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

package kafka.server

<<<<<<< HEAD
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, DefaultRecordBatch, MemoryRecords, RecordBatch, SimpleRecord}
=======
import kafka.message.{ByteBufferMessageSet, LZ4CompressionCodec, Message}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors, ProtoUtils}
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Subclasses of `BaseProduceSendRequestTest` exercise the producer and produce request/response. This class
  * complements those classes with tests that require lower-level access to the protocol.
  */
class ProduceRequestTest extends BaseRequestTest {

  @Test
  def testSimpleProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
<<<<<<< HEAD

    def sendAndCheck(memoryRecords: MemoryRecords, expectedOffset: Long): ProduceResponse.PartitionResponse = {
      val topicPartition = new TopicPartition("topic", partition)
      val partitionRecords = Map(topicPartition -> memoryRecords)
      val produceResponse = sendProduceRequest(leader,
          new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE, -1, 3000, partitionRecords.asJava).build())
      assertEquals(1, produceResponse.responses.size)
      val (tp, partitionResponse) = produceResponse.responses.asScala.head
      assertEquals(topicPartition, tp)
      assertEquals(Errors.NONE, partitionResponse.error)
      assertEquals(expectedOffset, partitionResponse.baseOffset)
      assertEquals(-1, partitionResponse.logAppendTime)
      partitionResponse
    }

    sendAndCheck(MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes)), 0)

    sendAndCheck(MemoryRecords.withRecords(CompressionType.GZIP,
      new SimpleRecord(System.currentTimeMillis(), "key1".getBytes, "value1".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key2".getBytes, "value2".getBytes)), 1)
=======
    val messageBuffer = new ByteBufferMessageSet(new Message("value".getBytes, "key".getBytes,
      System.currentTimeMillis(), 1: Byte)).buffer
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> messageBuffer)
    val produceResponse = sendProduceRequest(leader, new ProduceRequest(-1, 3000, partitionRecords.asJava))
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode)
    assertEquals(0, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.timestamp)
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
  }

  /* returns a pair of partition id and leader id */
  private def createTopicAndFindPartitionWithLeader(topic: String): (Int, Int) = {
    val partitionToLeader = TestUtils.createTopic(zkUtils, topic, 3, 2, servers)
    partitionToLeader.collectFirst {
<<<<<<< HEAD
      case (partition, leader) if leader != -1 => (partition, leader)
=======
      case (partition, Some(leader)) if leader != -1 => (partition, leader)
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
    }.getOrElse(fail(s"No leader elected for topic $topic"))
  }

  @Test
  def testCorruptLz4ProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
<<<<<<< HEAD
    val timestamp = 1000000
    val memoryRecords = MemoryRecords.withRecords(CompressionType.LZ4,
      new SimpleRecord(timestamp, "key".getBytes, "value".getBytes))
    // Change the lz4 checksum value (not the kafka record crc) so that it doesn't match the contents
    val lz4ChecksumOffset = 6
    memoryRecords.buffer.array.update(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + lz4ChecksumOffset, 0)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)
    val produceResponse = sendProduceRequest(leader, 
      new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE, -1, 3000, partitionRecords.asJava).build())
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.CORRUPT_MESSAGE, partitionResponse.error)
    assertEquals(-1, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.logAppendTime)
  }

  private def sendProduceRequest(leaderId: Int, request: ProduceRequest): ProduceResponse = {
    val response = connectAndSend(request, ApiKeys.PRODUCE, destination = brokerSocketServer(leaderId))
    ProduceResponse.parse(response, request.version)
=======
    val messageBuffer = new ByteBufferMessageSet(LZ4CompressionCodec, new Message("value".getBytes, "key".getBytes,
      System.currentTimeMillis(), 1: Byte)).buffer
    // Change the lz4 checksum value so that it doesn't match the contents
    messageBuffer.array.update(40, 0)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> messageBuffer)
    val produceResponse = sendProduceRequest(leader, new ProduceRequest(-1, 3000, partitionRecords.asJava))
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.CORRUPT_MESSAGE.code, partitionResponse.errorCode)
    assertEquals(-1, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.timestamp)
  }

  private def sendProduceRequest(leaderId: Int, request: ProduceRequest): ProduceResponse = {
    val socket = connect(s = servers.find(_.config.brokerId == leaderId).map(_.socketServer).getOrElse {
      fail(s"Could not find broker with id $leaderId")
    })
    val response = send(socket, request, ApiKeys.PRODUCE, ProtoUtils.latestVersion(ApiKeys.PRODUCE.id))
    ProduceResponse.parse(response)
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
  }

}
