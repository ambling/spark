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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 *
 * A manager that manage files in the /dev/shm for chronicle map
 */
private[spark] class ChronicleMapManager(
    conf: SparkConf,
    deleteFilesOnStop: Boolean)
  extends Logging {

  private val tmpfsDir = Utils.createDirectory("/dev/shm", "blockmgr")

  private val shutdownHook = addShutdownHook()

  def getFile(filename: String): File = {
    new File(tmpfsDir, filename)
  }

  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** Check if chroniclemap block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  def removeBlock(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    if (file.exists()) {
      val ret = file.delete()
      if (!ret) {
        logWarning(s"Error deleting ${file.getPath()}")
      }
      ret
    } else {
      false
    }
  }

  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      ChronicleMapManager.this.doStop()
    }
  }

  /** Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop() {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  private def doStop(): Unit = {
    if (deleteFilesOnStop) {
      if (tmpfsDir.isDirectory() && tmpfsDir.exists()) {
        try {
          if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(tmpfsDir)) {
            Utils.deleteRecursively(tmpfsDir)
          }
        } catch {
          case e: Exception =>
            logError(s"Exception while deleting local spark dir: $tmpfsDir", e)
        }
      }
    }
  }
}
