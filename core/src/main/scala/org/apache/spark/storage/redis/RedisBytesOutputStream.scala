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

import com.lambdaworks.redis.api.StatefulRedisConnection

/**
 * An output stream that write (append) bytes to a string value with a specific key
 */
private[spark] class RedisBytesOutputStream(
    val connection: StatefulRedisConnection[String, ByteBuffer],
    val key: String)
  extends OutputStream with Closeable {

  var closed: Boolean = false

  // use asynchronous commend to write data
  val asyncCommend = connection.async()

  override def write(b: Int): Unit = {
    asyncCommend.append(key, ByteBuffer.wrap(Array(b.toByte)))
  }

  override def write(b: Array[Byte]): Unit = {
    asyncCommend.append(key, ByteBuffer.wrap(b))
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    val buf = ByteBuffer.wrap(b, off, len)
    asyncCommend.append(key, buf)
  }

  override def flush(): Unit = {
    asyncCommend.flushCommands()
  }

  override def close(): Unit = {
    closed = true
    flush()
  }

  def getChannel: WritableByteChannel = {
    new RedisByteChannel(this)
  }
}

private[spark] class RedisByteChannel(stream: RedisBytesOutputStream) extends WritableByteChannel {

  override def write(src: ByteBuffer): Int = {
    stream.write(src.array(), src.position(), src.remaining())
    src.remaining()
  }

  override def isOpen: Boolean = stream.closed

  override def close(): Unit = stream.close()
}
