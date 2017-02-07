/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.common.driver.api;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Event from Driver Process to Driver Runtime.
 * A request to the Driver Runtime to release this resource
 */
@RuntimeAuthor
@DriverSide
@DefaultImplementation(ResourceReleaseEventImpl.class)
public interface ResourceReleaseEvent {
  /**
   * @return Id of the resource to release
   */
  String getIdentifier();

  /**
   * @return name of the runtime that this resource belongs to
   */
  String getRuntimeName();
}