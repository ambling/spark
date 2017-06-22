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

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.collection.JavaConverters._

import com.google.common.io.Closeables
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.sync.RedisCommands

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.redis.index.IndexWriter
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Store blocks on Redis.
 */
private[spark] class RedisStore(conf: SparkConf) extends Logging with AutoCloseable {

  val streamThreshold: Long = conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")

  /**
   * All local connections use unix domain sockets instead of TCP.
   */
  val sockpath: String = conf.get("spark.redis.sockpath", "/tmp/redis.sock")
  val redisClient: RedisClient = RedisClient.create(s"redis-socket://$sockpath")
  // save blocks as (blockId, ByteBuffer)
  val connection: StatefulRedisConnection[String, ByteBuffer] =
    redisClient.connect(new StringByteBufferCodec)
  val syncCommands: RedisCommands[String, ByteBuffer] = connection.sync

  def put(blockId: BlockId)(writeFunc: RedisBytesOutputStream => Unit): Unit = {
    if (contains(blockId)) {
      throw new IllegalStateException(s"Block $blockId is already present in the disk store")
    }
    logDebug(s"Attempting to put block $blockId")
    val startTime = System.currentTimeMillis
    val outputStream = new RedisBytesOutputStream(connection, blockId.name)
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

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    put(blockId) { outputStream =>
      val channel = outputStream.getChannel
      Utils.tryWithSafeFinally {
        bytes.writeFully(channel)
      } {
        channel.close()
      }
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
    val channel = new RedisBytesChannel(connection, blockId.name, false)
    Channels.newInputStream(channel)
  }

  def remove(blockId: BlockId): Boolean = {
    val deleted = syncCommands.del(blockId.name)
    if (deleted == 1) {
      val allKeys = IndexWriter.allIndexesKey(blockId)
      val indexNames = syncCommands.smembers(allKeys)
      val keys = indexNames.asScala.toSeq.map { name =>
        IndexWriter.indexKey(blockId, new String(name.array()))
      }
      syncCommands.del(allKeys)
      syncCommands.del(keys: _*)

      true
    } else {
      false
    }
  }

  def contains(blockId: BlockId): Boolean = {
    syncCommands.exists(blockId.name)
  }

  def exist(blockId: BlockId, indexName: String): Boolean = {
    val allKeys = IndexWriter.allIndexesKey(blockId)
    syncCommands.sismember(allKeys, ByteBuffer.wrap(indexName.getBytes))
  }

  override def close(): Unit = {
    connection.close()
    redisClient.shutdown()
  }
}
