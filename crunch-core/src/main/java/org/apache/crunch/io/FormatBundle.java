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
package org.apache.crunch.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A combination of an {@link InputFormat} or {@link OutputFormat} and any extra 
 * configuration information that format class needs to run.
 * 
 * <p>The {@code FormatBundle} allow us to let different formats act as
 * if they are the only format that exists in a particular MapReduce job, even
 * when we have multiple types of inputs and outputs within a single job.
 */
public class FormatBundle<K> implements Serializable, Writable, Configurable {

  private final Logger LOG = LoggerFactory.getLogger(FormatBundle.class);
  /**
   * A comma-separated list of properties whose value will be redacted.
   * MR config to redact job conf properties: https://issues.apache.org/jira/browse/MAPREDUCE-6741
   */
  private static final String MR_JOB_REDACTED_PROPERTIES = "mapreduce.job.redacted-properties";
  private static final String REDACTION_REPLACEMENT_VAL = "*********(redacted)";

  private final String FILESYSTEM_BLACKLIST_PATTERNS_KEY = "crunch.fs.props.blacklist.patterns";
  private final String[] FILESYSTEM_BLACKLIST_PATTERNS_DEFAULT =
      new String[] {
          "^fs\\.defaultFS$",
          "^fs\\.default\\.name$"};

  private final String FILESYSTEM_WHITELIST_PATTERNS_KEY = "crunch.fs.props.whitelist.patterns";
  private final String[] FILESYSTEM_WHITELIST_PATTERNS_DEFAULT =
      new String[] {
          "^fs\\..*",
          "^dfs\\..*"};

  private Class<K> formatClass;
  private Map<String, String> extraConf;
  private Configuration conf;
  private FileSystem fileSystem;
  
  public static <T> FormatBundle<T> fromSerialized(String serialized, Configuration conf) {
    ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(serialized));
    try {
      FormatBundle<T> bundle = new FormatBundle<T>();
      bundle.setConf(conf);
      bundle.readFields(new DataInputStream(bais));
      return bundle;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends InputFormat<?, ?>> FormatBundle<T> forInput(Class<T> inputFormatClass) {
    return new FormatBundle<T>(inputFormatClass);
  }
  
  public static <T extends OutputFormat<?, ?>> FormatBundle<T> forOutput(Class<T> outputFormatClass) {
    return new FormatBundle<T>(outputFormatClass);
  }
  
  public FormatBundle() {
    // For Writable support
  }

  @VisibleForTesting
  FormatBundle(Class<K> formatClass) {
    this.formatClass = formatClass;
    this.extraConf = Maps.newHashMap();
  }

  public FormatBundle<K> set(String key, String value) {
    this.extraConf.put(key, value);
    return this;
  }

  public FormatBundle<K> setFileSystem(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
    return this;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public Class<K> getFormatClass() {
    return formatClass;
  }

  public Configuration configure(Configuration conf) {
    // first configure fileystem properties
    Map<String, String> appliedFsProperties = configureFileSystem(conf);

    // then apply extraConf properties
    for (Map.Entry<String, String> e : extraConf.entrySet()) {
      String key = e.getKey();
      String value = e.getValue();
      conf.set(key, value);
      if (appliedFsProperties.get(key) != null) {
        LOG.info("{}={} from extraConf overrode {}={} from filesystem conf",
            new Object[] {key, value, key, appliedFsProperties.get(key)});
      }
    }
    return conf;
  }

  private Map<String,String> configureFileSystem(Configuration conf) {
    if (fileSystem == null) {
      return Collections.emptyMap();
    }

    Collection<Pattern> blacklistPatterns =
        compilePatterns(
            conf.getStrings(FILESYSTEM_BLACKLIST_PATTERNS_KEY,
                FILESYSTEM_BLACKLIST_PATTERNS_DEFAULT));
    Collection<Pattern> whitelistPatterns =
        compilePatterns(
            conf.getStrings(FILESYSTEM_WHITELIST_PATTERNS_KEY,
                FILESYSTEM_WHITELIST_PATTERNS_DEFAULT));

    Configuration fileSystemConf = fileSystem.getConf();
    Map<String, String> appliedProperties = new HashMap<>();
    Collection<String> redactedProperties = conf.getTrimmedStringCollection(MR_JOB_REDACTED_PROPERTIES);

    for (Entry<String, String> e : fileSystemConf) {
      String key = e.getKey();
      String value = fileSystemConf.get(key);
      String originalValue = conf.get(key);

      if (value.equals(originalValue)) {
        continue;
      }

      Pattern matchingBlacklistPattern = matchingPattern(key, blacklistPatterns);
      if (matchingBlacklistPattern != null) {
        LOG.info("{}={} matches blacklist pattern '{}', omitted",
            new Object[] {key, value, matchingBlacklistPattern});
        continue;
      }
      Pattern matchingWhitelistPattern = matchingPattern(key, whitelistPatterns);
      if (matchingWhitelistPattern == null) {
        LOG.info("{}={} matches no whitelist pattern from {}, omitted",
            new Object[] {key, value, whitelistPatterns});
        continue;
      }

      if (key.equals(DFSConfigKeys.DFS_NAMESERVICES)) {
        String[] originalArrayValue = conf.getStrings(key);
        if (originalValue != null) {
          String[] newValue = value != null ? value.split(",") : new String[0];
          String[] merged = mergeValues(originalArrayValue, newValue);
          LOG.info("Merged '{}' into '{}' with result '{}'",
              new Object[] {newValue, DFSConfigKeys.DFS_NAMESERVICES, merged});
          conf.setStrings(key, merged);
          appliedProperties.put(key, StringUtils.arrayToString(merged));
          continue;
        }
      }

      String message = "Applied {}={} from FS '{}'";
      if (originalValue != null) {
        message += ", overriding '{}'";
      }
      if (redactedProperties.contains(key)) {
        LOG.info(message,
            new Object[]{key, REDACTION_REPLACEMENT_VAL, fileSystem.getUri(), REDACTION_REPLACEMENT_VAL});
      } else {
        LOG.info(message,
            new Object[]{key, value, fileSystem.getUri(), originalValue});
      }
      conf.set(key, value);
      appliedProperties.put(key, value);
    }
    return appliedProperties;
  }

  private static Pattern matchingPattern(String s, Collection<Pattern> patterns) {
    for (Pattern pattern : patterns) {
      if (pattern.matcher(s).find()) {
        return pattern;
      }
    }
    return null;
  }

  private static Collection<Pattern> compilePatterns(String[] patterns) {
    Collection<Pattern> compiledPatterns = new ArrayList<>(patterns.length);
    for (String pattern : patterns) {
      compiledPatterns.add(Pattern.compile(pattern));
    }
    return compiledPatterns;
  }

  private static String[] mergeValues(String[] value1, String[] value2) {
    Set<String> values = Sets.newHashSet();
    values.addAll(Arrays.asList(value1));
    values.addAll(Arrays.asList(value2));
    return values.toArray(new String[0]);
  }

  public String serialize() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      DataOutputStream dos = new DataOutputStream(baos);
      write(dos);
      return Base64.encodeBase64String(baos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getName() {
    return formatClass.getSimpleName();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(formatClass)
        .append(fileSystem)
        .append(extraConf).toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof FormatBundle)) {
      return false;
    }
    FormatBundle<K> oib = (FormatBundle<K>) other;
    return Objects.equals(formatClass, oib.formatClass)
        && Objects.equals(fileSystem, oib.fileSystem)
        && Objects.equals(extraConf, oib.extraConf);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.formatClass = readClass(in);
    int ecSize = in.readInt();
    this.extraConf = Maps.newHashMap();
    for (int i = 0; i  < ecSize; i++) {
      String key = Text.readString(in);
      String value = Text.readString(in);
      extraConf.put(key, value);
    }
    boolean hasFilesystem;
    try {
      hasFilesystem = in.readBoolean();
    } catch (EOFException e) {
      // This can be a normal occurrence when Crunch is treated as a  cluster-provided
      // dependency and the version is upgraded.  Some jobs will have been submitted with
      // code that does not contain the filesystem field.  If those jobs run later with
      // this code that does contain the field, EOFException will occur trying to read
      // the non-existent field.
      LOG.debug("EOFException caught attempting to read filesystem field.  This condition"
          + "may temporarily occur with jobs that are submitted before but run after a"
          + "cluster-provided Crunch version upgrade.", e);
      hasFilesystem = false;
    }
    if (hasFilesystem) {
      String fileSystemUri = Text.readString(in);
      Configuration filesystemConf = new Configuration(false);
      filesystemConf.readFields(in);
      this.fileSystem = FileSystem.get(URI.create(fileSystemUri), filesystemConf);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, formatClass.getName());
    out.writeInt(extraConf.size());
    for (Map.Entry<String, String> e : extraConf.entrySet()) {
      Text.writeString(out, e.getKey());
      Text.writeString(out, e.getValue());
    }
    out.writeBoolean(fileSystem != null);
    if (fileSystem != null) {
      Text.writeString(out, fileSystem.getUri().toString());
      fileSystem.getConf().write(out);
    }
  }
  
  private Class readClass(DataInput in) throws IOException {
    String className = Text.readString(in);
    try {
      return conf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("readObject can't find class", e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
