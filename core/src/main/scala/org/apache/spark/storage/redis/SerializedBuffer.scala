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

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

/**
 * A buffer of serialized data. This is used to move serialization process to users.
 */
final class SerializedBuffer(var buffer: ByteBuffer) extends KryoSerializable {

  def length: Int = buffer.remaining()

  override def read(kryo: Kryo, input: Input): Unit = {
    val len = input.readInt()
    buffer = ByteBuffer.allocate(len)
    input.readBytes(buffer.array())
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeInt(length)
    output.writeBytes(buffer.array(), buffer.position(), buffer.remaining())
  }
}
