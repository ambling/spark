/*
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

package org.apache.spark.storage.redis

import java.io._
import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import com.google.common.io.Closeables
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.sync.RedisCommands

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.redis.index.IndexWriter
import org.apache.spark.util.NextIterator
import org.apache.spark.util.io.ChunkedByteBuffer



/**
 * Store blocks on Redis.
 */
private[spark] class RedisStore(conf: SparkConf, executor: String)
  extends Logging with AutoCloseable {

  val streamThreshold: Long = conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")

  // two redis server per node
  val redisServerId: Int = executor.last.toInt % 3 + 1

  /**
   * All local connections use unix domain sockets instead of TCP.
   */
  val sockpath: String = conf.get("spark.redis.sockpath", s"/tmp/redis.$redisServerId.sock")
  val redisClient: RedisClient = RedisClient.create(s"redis-socket://$sockpath")
  // save blocks as (blockId, ByteBuffer)
  val connection: StatefulRedisConnection[String, ByteBuffer] =
    redisClient.connect(new StringByteBufferCodec)
  val syncCommands: RedisCommands[String, ByteBuffer] = connection.sync

  def put(blockId: BlockId)(writeFunc: OutputStream => Unit): Unit = {
    if (contains(blockId)) {
      throw new IllegalStateException(s"Block $blockId is already present in the disk store")
    }
    logDebug(s"Attempting to put block $blockId")
    val startTime = System.currentTimeMillis
    val outputStream =
      new BufferedOutputStream(new RedisBytesOutputStream(connection, blockId.name))
    var threwException: Boolean = true
    try {
      writeFunc(outputStream)
      threwException = false
    } finally {
      try {
        Closeables.close(outputStream, threwException)
      } finally {
        if (threwException) {
          remove(blockId)
        }
      }
    }
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored on redis in %d ms".format(
      blockId.name,
      finishTime - startTime))
  }

  def putBuffers(output: OutputStream, iter: Iterator[SerializedBuffer]): Unit = {
    val dataStream = new DataOutputStream(output)
    iter.foreach { data =>
      dataStream.writeInt(data.length)
      dataStream.write(data.buffer)
    }
    dataStream.flush()
  }

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    put(blockId) { outputStream =>
      bytes.getChunks().foreach(buf =>
        outputStream.write(buf.array, buf.arrayOffset + buf.position, buf.remaining))
    }
  }

  def getSize(blockId: BlockId): Long = {
    syncCommands.strlen(blockId.name)
  }

  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    val data = syncCommands.get(blockId.name)
    new ChunkedByteBuffer(data)
  }

  def getInputStream(blockId: BlockId): InputStream = {
    new BufferedInputStream(new RedisBytesInputStream(connection, blockId.name))
  }

  def getBuffers(input: InputStream): Iterator[SerializedBuffer] = {
    val dataInput = new DataInputStream(input)
    new NextIterator[SerializedBuffer] {

      override protected def getNext(): SerializedBuffer = {
        try {
          val length = dataInput.readInt()
          val data = new Array[Byte](length)
          dataInput.readFully(data)
          new SerializedBuffer(data)
        } catch {
          case eof: EOFException =>
            finished = true
            null
        }
      }

      override protected def close(): Unit = {
        dataInput.close()
      }
    }
  }

  def remove(blockId: BlockId): Boolean = {
    val allkeys = syncCommands.keys(blockId.name + "*").asScala
    if (allkeys.nonEmpty && syncCommands.del(allkeys: _*) > 0) {
      true
    } else {
      false
    }
  }

  def contains(blockId: BlockId): Boolean = {
    val exist = syncCommands.exists(Seq(blockId.name): _*)
    exist == 1
  }

  def exist(blockId: BlockId, indexName: String): Boolean = {
    val allKeys = IndexWriter.allIndexesKey(blockId)
    syncCommands.sismember(allKeys, ByteBuffer.wrap(indexName.getBytes))
  }

  override def close(): Unit = {
    syncCommands.flushall()
    connection.close()
    redisClient.shutdown()
  }
}
