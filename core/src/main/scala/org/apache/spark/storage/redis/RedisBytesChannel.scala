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

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, SeekableByteChannel}
import java.nio.channels.spi.AbstractInterruptibleChannel

import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.sync.RedisCommands

/**
 * An seekable channel that reads from and write to the Redis store with a secific key.
 */
private[spark] class RedisBytesChannel(
    connection: StatefulRedisConnection[String, ByteBuffer],
    val key: String,
    val appendOnly: Boolean = false)
  extends AbstractInterruptibleChannel with SeekableByteChannel {

  // use synchronous command to operate data
  private val syncCommand: RedisCommands[String, ByteBuffer] = connection.sync()
  private var pos: Long = if (appendOnly) size() else 0L

  private val TRANSFER_SIZE = 8192

  override def read(dst: ByteBuffer): Int = {
    val buf = syncCommand.getrange(key, pos, pos + dst.remaining() - 1)
    val size = buf.remaining()
    dst.put(buf)
    pos += size
    size
  }

  override def size(): Long = {
    if (!isOpen) throw new ClosedChannelException
    syncCommand.strlen(key)
  }

  override def truncate(size: Long): SeekableByteChannel = {
    throw new IOException("data in Redis is prohibite to truncate")
  }

  override def position(): Long = pos

  override def position(newPosition: Long): SeekableByteChannel = {
    if (!isOpen) throw new ClosedChannelException
    if (newPosition < 0) throw new IllegalArgumentException
    pos = newPosition
    this
  }

  override def write(src: ByteBuffer): Int = {
    val len = src.remaining
    var totalWritten = 0
    this.synchronized {
      while (totalWritten < len) {
        val bytesToWrite = Math.min(len - totalWritten, TRANSFER_SIZE)
        try {
          begin()
          val buf = src.duplicate()
          buf.limit(buf.position() + bytesToWrite)
          if (appendOnly) {
            syncCommand.append(key, buf)
          } else {
            syncCommand.setrange(key, pos, buf)
          }
          src.position(src.position() + bytesToWrite)
        } finally end(bytesToWrite > 0)
        totalWritten += bytesToWrite
      }
      totalWritten
    }
  }

  override def implCloseChannel(): Unit = {
  }
}
