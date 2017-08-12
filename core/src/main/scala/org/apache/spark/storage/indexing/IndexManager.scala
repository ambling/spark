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

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId


private[spark] class IndexManager extends Logging {

  @GuardedBy("this")
  private[this] val indexes = new mutable.HashMap[BlockId, mutable.HashMap[String, BlockIndex]]

  def getIndex(blockId: BlockId, name: String): Option[BlockIndex] = {
    indexes.get(blockId).flatMap(_.get(name))
  }

  def putIndex(blockId: BlockId, name: String, index: BlockIndex): Unit = {
    val indexMap = indexes.getOrElse(blockId, new mutable.HashMap[String, BlockIndex]())
    val oldIndex = indexMap.put(name, index)
    if (oldIndex.isDefined) oldIndex.get.close()
    indexes.put(blockId, indexMap)
  }

  def removeIndex(blockId: BlockId, name: String): Boolean = {
    indexes.get(blockId).forall { map =>
      val index = map.remove(name)
      index match {
        case Some(idx) =>
          idx.close()
          true
        case None =>
          false
      }
    }
  }

  def removeBlock(blockId: BlockId): Unit = {
    val indexMap = indexes.remove(blockId)
    indexMap match {
      case Some(map) =>
        map.values.foreach(_.close())
        true
      case None =>
        false
    }
  }
}
