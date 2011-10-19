package com.cloudera.crunch.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.crunch.SourceTarget;

/**
 * An interface that indicates that a {@code SourceTarget} instance can be
 * read into the local client.
 *
 * @param <T> The type of data read.
 */
public interface ReadableSourceTarget<T> extends SourceTarget<T> {
  Iterable<T> read(Configuration conf) throws IOException;
}
