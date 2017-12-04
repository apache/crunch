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
 * The package of classes here is needed to extend the default classes provided
 * by Hive. The classes in that package are package private, and therefore could
 * not be overridden outside of that package scope. Crunch needs to extend the
 * classes to override the behavior of creating
 * {@link org.apache.hadoop.mapred.TaskAttemptID}'s.
 *
 * {@link org.apache.hadoop.mapred.TaskAttemptID#forName(java.lang.String)} is
 * used by default in
 * {@link org.apache.hive.hcatalog.mapreduce.DefaultOutputCommitterContainer}
 * and {@link org.apache.hive.hcatalog.mapreduce.FileOutputFormatContainer} to
 * translate between MR v1 and MR v2. This causes issues because a TaskAttemptID
 * requires the string representation to be 6 elements, separated by underscores
 * ('_'). Crunch adds the named output to the JobID (which is used when creating
 * the TaskAttemptID) which gives the TaskAttemptID 7 elements.
 *
 * e.g.
 * 
 * <pre>
 * attempt_1508401628996_out0_16350_m_000000_0
 * </pre>
 *
 * So, the crunch classes in this package change the logic when creating
 * TaskAttemptID's to strip the named output before creating the TaskAttemptID.
 *
 * e.g
 *
 * <pre>
 *     attempt_1508401628996_out0_16350_m_000000_0 -> attempt_1508401628996_16350_m_000000_0
 * </pre>
 */
package org.apache.hive.hcatalog.mapreduce;