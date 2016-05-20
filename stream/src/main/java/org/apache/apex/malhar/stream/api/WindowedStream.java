/**
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
package org.apache.apex.malhar.stream.api;

/**
 * <p>
 * A stream with windowed transformation
 * </p>
 * <p>
 * <B>Transformation types:</B>
 * <ul>
 * <li>Combine</li>
 * <li>Group</li>
 * <li>Keyed Combine</li>
 * <li>Keyed Group</li>
 * <li>Join</li>
 * <li>CoGroup</li>
 * </ul>
 * </p>
 * <p>
 * <B>Features supported with windowed transformation </B>
 * <ul>
 * <li>Watermark - Ingestion time watermark / logical tuple watermark</li>
 * <li>Early Triggers - How frequent to emit real-time partial result</li>
 * <li>Late Triggers - When to emit updated result with tuple comes after watermark</li>
 * <li>Customizable Trigger Behaviour - What to do when fires a trigger</li>
 * <li>Spool window state -  In-Memory window state can be spooled to disk if it is full</li>
 * <li>3 different accumulation models: ignore, accumulation, accumulation + delta</li>
 * <li>Window support: Non-Mergeable window(fix window, sliding window), Mergeable window(session window) base on 3 different tuple time</li>
 * <li>Different tuple time support: event time, system time, ingestion time</li>
 * </ul>
 * </p>
 *
 * @param <T> Output tuple type
 */
public interface WindowedStream<T> extends ApexStream<T>
{
}
