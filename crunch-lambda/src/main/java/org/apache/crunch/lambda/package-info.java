/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <p>Alternative Crunch API using Java 8 features to allow construction of pipelines using lambda functions and method
 * references. It works by wrapping standards Java {@link org.apache.crunch.PCollection},
 * {@link org.apache.crunch.PTable} and {@link org.apache.crunch.PGroupedTable} instances into the corresponding
 * {@link org.apache.crunch.lambda.LCollection}, {@link org.apache.crunch.lambda.LTable} and
 * {@link org.apache.crunch.lambda.LGroupedTable classes}.</p>
 *
 * <p>The static class {@link org.apache.crunch.lambda.Lambda} has methods to create these. Please also see the Javadocs
 * for {@link org.apache.crunch.lambda.Lambda} for usage examples</p>
 */
package org.apache.crunch.lambda;

