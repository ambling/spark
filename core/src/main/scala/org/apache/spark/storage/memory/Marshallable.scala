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

import net.openhft.chronicle.hash.serialization._

/**
 * A trait that can get the marshaller from the instance.
 *
 * Note: if the concrete class dose not have a no-arg constructor, these marshallers should
 * implement 'InstanceCreatingMarshaller' to get a default instance.
 */
trait Marshallable {
  def getSizedReader: SizedReader[Any]

  def getSizedWriter: SizedWriter[Any]

  def getBytesReader: BytesReader[Any]

  def getBytesWriter: BytesWriter[Any]

  def getSizeMarshaller: SizeMarshaller
}
