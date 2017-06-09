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

import com.google.common.io.Closeables
import com.lambdaworks.redis.RedisClient

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Store blocks on Redis.
 */
private[spark] class RedisStore(conf: SparkConf) extends Logging with AutoCloseable {

  /**
   * All local connections use unix domain sockets instead of TCP.
   */
  val sockpath = conf.get("spark.redis.sockpath", "/tmp/redis.sock")
  val redisClient = RedisClient.create(s"redis-socket://$sockpath")
  // save blocks as (blockId, ByteBuffer)
  val connection = redisClient.connect(new StringByteBufferCodec)
  val syncCommends = connection.sync

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
    syncCommends.strlen(blockId.name)
  }

  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    val data = syncCommends.get(blockId.name)
    new ChunkedByteBuffer(data)
  }

  def remove(blockId: BlockId): Boolean = {
    val deleted = syncCommends.del(Seq(blockId.name): _*)
    deleted == 1
  }

  def contains(blockId: BlockId): Boolean = {
    val existed = syncCommends.exists(Seq(blockId.name): _*)
    existed == 1
  }

  override def close(): Unit = {
    connection.close()
    redisClient.shutdown()
  }
}
