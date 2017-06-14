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

import java.io.{Closeable, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.nio.channels.spi.AbstractInterruptibleChannel

import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.sync.RedisCommands

/**
 * An output stream that write (append) bytes to a string value with a specific key
 */
private[spark] class RedisBytesOutputStream(
    val connection: StatefulRedisConnection[String, ByteBuffer],
    val key: String)
  extends OutputStream with Closeable {

  val channel = new RedisBytesChannel(connection, key, true)

  override def write(b: Int): Unit = {
    val buf = ByteBuffer.allocate(4)
    buf.putInt(b)
    buf.flip()
    buf.position(3)
    write(buf)
  }

  override def write(b: Array[Byte]): Unit = {
    write(ByteBuffer.wrap(b))
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    val buf = ByteBuffer.wrap(b, off, len)
    write(buf)
  }

  /**
   * convenient write content in a bytebuffer
   */
  def write(buf: ByteBuffer): Unit = {
    channel.write(buf)
  }

  override def flush(): Unit = {
  }

  override def close(): Unit = {
    channel.close()
  }

  def getChannel: WritableByteChannel = {
    channel
  }
}

