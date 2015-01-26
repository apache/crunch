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
package org.apache.crunch.io.text.xml;

import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;

/**
 * Large XML documents composed of repetitive XML elements can be broken into chunks delimited by element's start and
 * end tag. The {@link XmlSource2} process XML files and extract out the XML between the pre-configured start / end
 * tags. Developer should process the content between the tags.
 * 
 * The {@link XmlSource} does not parse the input XML files and is not aware of the XML semantics. It just splits the
 * input file in chunks defined by the start/end tags. Nested XML elements are not supported.
 */
public class XmlSource extends FileSourceImpl<String> {

  /**
   * Create new XML data loader using the UTF-8 encoding.
   * 
   * @param inputPath
   *          Input XML file location
   * @param tagStart
   *          Elements's start tag
   * @param tagEnd
   *          Elements's end tag
   */
  public XmlSource(String inputPath, String tagStart, String tagEnd) {
    this(inputPath, tagStart, tagEnd, Charsets.UTF_8.name());
  }

  /**
   * Create new XML data loader using the specified encoding.
   * 
   * @param inputPath
   *          Input XML file location
   * @param tagStart
   *          Elements's start tag
   * @param tagEnd
   *          Elements's end tag
   * @param encoding
   *          Input file encoding
   */
  public XmlSource(String inputPath, String tagStart, String tagEnd, String encoding) {
    super(new Path(inputPath), 
        Writables.strings(), 
        FormatBundle.forInput(XmlInputFormat.class)
          .set(XmlInputFormat.START_TAG_KEY, tagStart)
          .set(XmlInputFormat.END_TAG_KEY, tagEnd)
          .set(XmlInputFormat.ENCODING, encoding));
  }
}
