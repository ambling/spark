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

import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.lambdaworks.redis.codec.RedisCodec
import io.netty.buffer.Unpooled

/**
 * A codec for redis client.
 * Note: the key should only encoded with ASCII charset
 */
class StringByteBufferCodec extends RedisCodec[String, ByteBuffer] {

  val charset: Charset = Charset.forName("US-ASCII")

  override def decodeKey(bytes: ByteBuffer): String = {
    Unpooled.wrappedBuffer(bytes).toString(charset)
  }

  override def encodeKey(key: String): ByteBuffer = {
    ByteBuffer.wrap(key.getBytes)
  }

  override def encodeValue(value: ByteBuffer): ByteBuffer = {
    if (value == null) ByteBuffer.wrap(Array[Byte]())
    else value.duplicate()
  }

  override def decodeValue(bytes: ByteBuffer): ByteBuffer = {
    val data = new Array[Byte](bytes.remaining())
    bytes.get(data)
    ByteBuffer.wrap(data)
  }

}
