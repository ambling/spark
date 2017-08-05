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

package org.apache.spark.storage.memory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import net.openhft.chronicle.bytes.Bytes
import net.openhft.chronicle.core.values.IntValue
import net.openhft.chronicle.hash.serialization._
import net.openhft.chronicle.map.ChronicleMap
import net.openhft.chronicle.values.Values

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.storage.BlockId

/**
 *
 * Store key-values with an off-heap fashion on the Chronicle Map.
 */
private[spark] class ChronicleMapStore(
    val manager: ChronicleMapManager,
    val serializerManager: SerializerManager)
  extends Logging {

  private[this] val blocks =
    new mutable.HashMap[BlockId, mutable.HashMap[String, ChronicleMap[Any, Any]]]

  def getKVBlock(blockId: BlockId): Option[ChronicleMap[Any, Any]] = {
    blocks.get(blockId).flatMap(_.get("block"))
  }

  def getKVIndex(blockId: BlockId, name: String): Option[ChronicleMap[Any, Any]] = {
    blocks.get(blockId).flatMap(_.get(name))
  }

  def putKVIndex(blockId: BlockId, name: String, index: ChronicleMap[Any, Any]): Boolean = {
    if (blocks.contains(blockId)) {
      blocks(blockId).put(name, index)
      true
    }
    else false
  }

  def getKVIndexPath(blockId: BlockId, name: String): String = {
    manager.getIndexFile(blockId, name).getPath
  }

  def getValues[T](blockId: BlockId,
                   classTag: ClassTag[T]): Iterator[Any] = {
    if (!blocks.contains(blockId)) {
      logError( s"block ${blockId.name} dose not exist on this executor.")
      return Iterator[Any]()
    }
    val map = blocks(blockId)("block")
    map.values().iterator().asScala
  }

  def putIteratorAsValues[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T]): Either[Iterator[T], Long] = {
    var map: ChronicleMap[Any, Any] = null
    var seq: Seq[T] = null
    try {
      // since we need to check the size, the iterator is firstly changed to a Seq
      seq = values.toSeq
      map = createChronicleMap(blockId, seq, classTag).asInstanceOf[ChronicleMap[Any, Any]]
      if (blocks.contains(blockId)) blocks(blockId).put("block", map)
      else {
        val newMap = mutable.HashMap[String, ChronicleMap[Any, Any]](("block", map))
        blocks.put(blockId, newMap)
      }
      val size = getSize(blockId)
      Right(size)
    } catch {
      case e: Exception =>
        logError("Fail to put values into KV:\n" + e.getStackTrace.mkString("\n"))
        if (map != null) {
          map.close()
          remove(blockId)
        }
        if (seq != null) Left(seq.iterator)
        else Left(values)
    }
  }

  def createChronicleMap[T](
      blockId: BlockId,
      values: Seq[T],
      classTag: ClassTag[T]): ChronicleMap[IntValue, Any] = {

    val file = manager.getFile(blockId)
    val clazz = classTag.runtimeClass.asInstanceOf[Class[T]]
    val builder = ChronicleMap
      .of(classOf[IntValue], clazz)
      .averageValue(values.head)
      .entries(values.size)
    val map =
      if (classOf[Marshallable].isAssignableFrom(clazz)) {
        val instance = values.head.asInstanceOf[Marshallable]
        if (instance.getSizedReader != null) {
          val reader = instance.getSizedReader.asInstanceOf[SizedReader[T]]
          val writer = instance.getSizedWriter.asInstanceOf[SizedWriter[T]]
          builder.valueMarshallers(reader, writer)
        } else {
          val reader = instance.getBytesReader.asInstanceOf[BytesReader[T]]
          val writer = instance.getBytesWriter.asInstanceOf[BytesWriter[T]]
          builder.valueMarshallers(reader, writer)
        }
        if (instance.getSizeMarshaller != null) {
          builder.valueSizeMarshaller(instance.getSizeMarshaller)
        }
        builder.createPersistedTo(file)
      } else {
        val serializer = serializerManager.getSerializer(classTag, autoPick = true).newInstance()
        val marshaller = new SerializerMarshaller[T](serializer)(classTag)
        builder.valueMarshaller(marshaller).createPersistedTo(file)
      }

    val k = Values.newHeapInstance(classOf[IntValue])
    k.setValue(0)
    for (v <- values) {
      map.put(k, v)
      k.addValue(1)
    }
    map.asInstanceOf[ChronicleMap[IntValue, Any]]

  }

  def getSize(blockId: BlockId): Long = {
    manager.getFile(blockId.name).length
  }

  def remove(blockId: BlockId): Boolean = {
    var indexNames = Iterable[String]()
    if (blocks.contains(blockId)) {
      indexNames = blocks(blockId).keys
      blocks(blockId).values.foreach(_.close())
    }
    manager.removeBlockWithIndexes(blockId, indexNames)
  }

  def contains(blockId: BlockId): Boolean = {
    val file = manager.getFile(blockId.name)
    file.exists()
  }

  def clear(): Unit = {
    blocks.keys.foreach(block => remove(block))
  }
}

class SerializerMarshaller[T: ClassTag](val serializer: SerializerInstance)
  extends BytesWriter[T] with BytesReader[T] {

  override def write(out: Bytes[_], toWrite: T): Unit = {
    val os = out.outputStream()
    serializer.serializeStream(os).writeObject(toWrite)
  }

  override def read(in: Bytes[_], using: T): T = {
    val is = in.inputStream()
    serializer.deserializeStream(is).readObject()
  }
}
