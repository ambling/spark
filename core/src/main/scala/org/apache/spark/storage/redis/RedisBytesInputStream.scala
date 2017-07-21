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

import com.lambdaworks.redis.api.StatefulRedisConnection

/**
 * Input Stream that read from a redis value.
 */
class RedisBytesInputStream(
    val connection: StatefulRedisConnection[String, ByteBuffer],
    val key: String)
  extends InputStream {

  private val syncCommand = connection.sync()
  private var pos = 0L
  private val size = syncCommand.strlen(key)

  override def read(): Int = {
    val buf = syncCommand.getrange(key, pos, pos)
    pos += buf.remaining()
    buf.get() & 0xff
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val buf = syncCommand.getrange(key, pos, pos + len - 1)
    val hasRead = buf.remaining()
    buf.get(b, off, buf.remaining())
    pos += hasRead
    hasRead
  }

  override def skip(n: Long): Long = {
    val newpos = Math.min(size - 1, pos + n)
    val skipped = newpos - pos
    pos = newpos
    skipped
  }

  override def available(): Int = {
    Math.min(size - pos, Integer.MAX_VALUE).toInt
  }
}
