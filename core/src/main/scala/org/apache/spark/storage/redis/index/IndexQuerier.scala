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

package org.apache.spark.storage.redis.index

import java.nio.ByteBuffer

import com.lambdaworks.redis.api.StatefulRedisConnection

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.BlockId

/**
 * A helper class to query index nodes from a Redis store.
 */
@DeveloperApi
class IndexQuerier(
    val connection: StatefulRedisConnection[String, ByteBuffer],
    val blockId: BlockId,
    val indexName: String) {

  private val syncCommand = connection.sync()
  private val key = IndexWriter.indexKey(blockId, indexName)

  def getNode(nodeKey: String): ByteBuffer = {
    syncCommand.hget(key, nodeKey)
  }
}
