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

import java.io.IOException

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
import org.apache.spark.storage.{BlockId, DiskBlockManager}

/**
 *
 * Store key-values with an off-heap fashion on the Chronicle Map.
 */
private[spark] class ChronicleMapStore(
    val manager: ChronicleMapManager,
    val diskManager: DiskBlockManager,
    val serializerManager: SerializerManager)
  extends Logging {

  private[this] val blocks = new mutable.HashMap[BlockId, ChronicleMap[Any, Any]]

  def getKVBlock(blockId: BlockId): Option[ChronicleMap[Any, Any]] = {
    blocks.get(blockId)
  }

  /**
   * Since the map creation is called by user,
   * this method provides a path on disk in case the memory is not enouph.
   * @return (InMemPath, OnDiskPath)
   */
  def getKVIndexPath(blockId: BlockId, name: String): (String, String) = {
    (manager.getIndexFile(blockId, name).getPath,
      diskManager.getFile(blockId.name + "_" + name).getPath)
  }

  def getValues[T](blockId: BlockId,
                   classTag: ClassTag[T]): Iterator[Any] = {
    if (!blocks.contains(blockId)) {
      logError( s"block ${blockId.name} dose not exist on this executor.")
      return Iterator[Any]()
    }
    val map = blocks(blockId)
    map.values().iterator().asScala
  }

  def putIteratorAsValues[T](
      blockId: BlockId,
      onDisk: Boolean,
      values: Iterator[T],
      classTag: ClassTag[T]): Either[Iterator[T], Long] = {
    // this method will always return a Right(Long) because we handle the case in map creation.
    // since we need to check the size, the iterator is firstly changed to a Seq.
    val seq = values.toSeq

    try {
      if (blocks.contains(blockId)) blocks(blockId).close() // close old before create new map
      val map = createChronicleMap(blockId, onDisk, seq, classTag)
        .asInstanceOf[ChronicleMap[Any, Any]]
      blocks.put(blockId, map)
      val size = getMapSize(blockId)
      Right(size)
    } catch {
      case e: IOException =>
        logError(s"fail to persist block ${blockId.name} in chronicle map, due to: ${e.getMessage}")
        Left(values)
    }


  }

  def createChronicleMap[T](
      blockId: BlockId,
      onDisk: Boolean,
      values: Seq[T],
      classTag: ClassTag[T]): ChronicleMap[IntValue, Any] = {

    val file = manager.getFile(blockId)
    if (file.exists()) file.delete()

    val diskFile = diskManager.getFile(blockId)
    diskFile.mkdirs()
    if (diskFile.exists()) diskFile.delete()

    val clazz = classTag.runtimeClass.asInstanceOf[Class[T]]
    val builder = ChronicleMap
      .of(classOf[IntValue], clazz)

    if (values.isEmpty) {
      // for empty map
      builder.entries(1).averageValueSize(1)
    } else {
      builder.entries((values.size*1.2).toLong)

      if (classOf[Marshallable].isAssignableFrom(clazz)) {
        val instance = values.head.asInstanceOf[Marshallable]
        val comparator = instance.getSizeComparator
        val sorted = values.asInstanceOf[Seq[Marshallable]]
          .sorted(Ordering.comparatorToOrdering(comparator).reverse)
        builder.averageValue(sorted.head.asInstanceOf[T])
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
      } else {
        val serializer = serializerManager.getSerializer(classTag, autoPick = true).newInstance()
        val marshaller = new SerializerMarshaller[T](serializer)(classTag)
        builder
          .averageValue(values.head)
          .valueMarshaller(marshaller)
      }
    }

    val map = if (onDisk) builder.createPersistedTo(diskFile) else {
      try {
        builder.createPersistedTo(file)
      } catch {
        case e: IOException =>
          builder.createPersistedTo(diskFile)
      }
    }

    val k = Values.newHeapInstance(classOf[IntValue])
    k.setValue(0)
    for (v <- values) {
      map.put(k, v)
      k.addValue(1)
    }
    map.asInstanceOf[ChronicleMap[IntValue, Any]]

  }

  /**
   * Returns the data size in memory.
   * @param blockId
   */
  def getSize(blockId: BlockId): Long = {
    if (manager.containsBlock(blockId)) {
      manager.getFile(blockId.name).length
    }
    else if (diskManager.containsBlock(blockId)) {
      diskManager.getFile(blockId).length()
    }
    else 0L
  }

  /**
   * Returns the data size of this chroniclemap, can be in memory or on disk.
   */
  def getMapSize(blockId: BlockId): Long = {
    if (manager.containsBlock(blockId)) manager.getFile(blockId.name).length
    else if (diskManager.containsBlock(blockId)) diskManager.getFile(blockId).length()
    else 0L
  }

  def remove(blockId: BlockId): Boolean = {
    if (blocks.contains(blockId)) {
      blocks(blockId).close()
    }
    manager.removeBlock(blockId) ||
      (diskManager.containsBlock(blockId) && diskManager.getFile(blockId).delete())
  }

  def contains(blockId: BlockId): Boolean = {
    blocks.contains(blockId)
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
