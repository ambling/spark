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

package org.apache.spark.storage.indexing

import java.util.concurrent.ConcurrentHashMap
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId


private[spark] class IndexManager extends Logging {

  @GuardedBy("this")
  private[this] val indexes = new ConcurrentHashMap[BlockId, mutable.HashMap[String, BlockIndex]]

  def getIndex(blockId: BlockId, name: String): Option[BlockIndex] = {
    val map = indexes.get(blockId)
    if (map != null) map.get(name)
    else None
  }

  def putIndex(blockId: BlockId, name: String, index: BlockIndex): Unit = {
    val indexMap = indexes.getOrDefault(blockId, new mutable.HashMap[String, BlockIndex]())
    val oldIndex = indexMap.put(name, index)
    if (oldIndex.isDefined) oldIndex.get.close()
    indexes.put(blockId, indexMap)
  }

  def removeIndex(blockId: BlockId, name: String): Boolean = {
    val map = indexes.get(blockId)
    if (map != null) {
      val index = map.remove(name)
      index match {
        case Some(idx) =>
          idx.close()
          true
        case None =>
          false
      }
    } else false
  }

  def removeBlock(blockId: BlockId): Unit = {
    val map = indexes.remove(blockId)
    if (map != null) {
      map.values.foreach(_.close())
    }
  }
}
