/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.crunch.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.tools.CopyListing;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.SimpleCopyListing;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * A custom {@link CopyListing} implementation capable of dynamically renaming
 * the target paths according to a {@link #DISTCP_PATH_RENAMES configured set of values}.
 * <p>
 * Once https://issues.apache.org/jira/browse/HADOOP-16147 is available, this
 * class can be significantly simplified.
 * </p>
 */
public class CrunchRenameCopyListing extends SimpleCopyListing {
  /**
   * Comma-separated list of original-file:renamed-file path rename pairs.
   */
  public static final String DISTCP_PATH_RENAMES = "crunch.distcp.path.renames";

  private static final Logger LOG = LoggerFactory.getLogger(CrunchRenameCopyListing.class);
  private final Map<String, String> pathRenames;

  private long totalPaths = 0;
  private long totalBytesToCopy = 0;

  /**
   * Constructor, to initialize configuration.
   *
   * @param configuration The input configuration, with which the source/target FileSystems may be accessed.
   * @param credentials - Credentials object on which the FS delegation tokens are cached. If null
   * delegation token caching is skipped
   */
  public CrunchRenameCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);

    pathRenames = new HashMap<>();

    String[] pathRenameConf = configuration.getStrings(DISTCP_PATH_RENAMES);
    if (pathRenameConf == null) {
      throw new IllegalArgumentException("Missing required configuration: " + DISTCP_PATH_RENAMES);
    }
    for (String pathRename : pathRenameConf) {
      String[] pathRenameParts = pathRename.split(":");
      if (pathRenameParts.length != 2) {
        throw new IllegalArgumentException("Invalid path rename format: " + pathRename);
      }
      if (pathRenames.put(pathRenameParts[0], pathRenameParts[1]) != null) {
        throw new IllegalArgumentException("Invalid duplicate path rename: " + pathRenameParts[0]);
      }
    }
    LOG.info("Loaded {} path rename entries", pathRenames.size());

    // Clear out the rename configuration property, as it is no longer needed
    configuration.unset(DISTCP_PATH_RENAMES);
  }

  @Override
  public void doBuildListing(SequenceFile.Writer fileListWriter, DistCpOptions options) throws IOException {
    try {
      for (Path path : options.getSourcePaths()) {
        FileSystem sourceFS = path.getFileSystem(getConf());
        final boolean preserveAcls = options.shouldPreserve(FileAttribute.ACL);
        final boolean preserveXAttrs = options.shouldPreserve(FileAttribute.XATTR);
        final boolean preserveRawXAttrs = options.shouldPreserveRawXattrs();
        path = makeQualified(path);

        FileStatus rootStatus = sourceFS.getFileStatus(path);
        Path sourcePathRoot = computeSourceRootPath(rootStatus, options);

        FileStatus[] sourceFiles = sourceFS.listStatus(path);
        boolean explore = (sourceFiles != null && sourceFiles.length > 0);
        if (!explore || rootStatus.isDirectory()) {
          CopyListingFileStatus rootCopyListingStatus = DistCpUtils.toCopyListingFileStatus(sourceFS, rootStatus, preserveAcls,
              preserveXAttrs, preserveRawXAttrs);
          writeToFileListingRoot(fileListWriter, rootCopyListingStatus, sourcePathRoot, options);
        }
        if (explore) {
          for (FileStatus sourceStatus : sourceFiles) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Recording source-path: {} for copy.", sourceStatus.getPath());
            }
            CopyListingFileStatus sourceCopyListingStatus = DistCpUtils.toCopyListingFileStatus(sourceFS, sourceStatus,
                preserveAcls && sourceStatus.isDirectory(), preserveXAttrs && sourceStatus.isDirectory(),
                preserveRawXAttrs && sourceStatus.isDirectory());
            writeToFileListing(fileListWriter, sourceCopyListingStatus, sourcePathRoot, options);

            if (isDirectoryAndNotEmpty(sourceFS, sourceStatus)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Traversing non-empty source dir: {}", sourceStatus.getPath());
              }
              traverseNonEmptyDirectory(fileListWriter, sourceStatus, sourcePathRoot, options);
            }
          }
        }
      }
      fileListWriter.close();
      fileListWriter = null;
    } finally {
      if (fileListWriter != null) {
        try {
          fileListWriter.close();
        } catch(IOException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Exception in closing {}", fileListWriter, e);
          }
        }
      }
    }
  }

  private Path computeSourceRootPath(FileStatus sourceStatus, DistCpOptions options) throws IOException {
    Path target = options.getTargetPath();
    FileSystem targetFS = target.getFileSystem(getConf());
    final boolean targetPathExists = options.getTargetPathExists();

    boolean solitaryFile = options.getSourcePaths().size() == 1 && !sourceStatus.isDirectory();

    if (solitaryFile) {
      if (targetFS.isFile(target) || !targetPathExists) {
        return sourceStatus.getPath();
      } else {
        return sourceStatus.getPath().getParent();
      }
    } else {
      boolean specialHandling =
          (options.getSourcePaths().size() == 1 && !targetPathExists) || options.shouldSyncFolder() || options.shouldOverwrite();

      return specialHandling && sourceStatus.isDirectory() ? sourceStatus.getPath() : sourceStatus.getPath().getParent();
    }
  }

  private Path makeQualified(Path path) throws IOException {
    final FileSystem fs = path.getFileSystem(getConf());
    return path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  private static boolean isDirectoryAndNotEmpty(FileSystem fileSystem, FileStatus fileStatus) throws IOException {
    return fileStatus.isDirectory() && getChildren(fileSystem, fileStatus).length > 0;
  }

  private static FileStatus[] getChildren(FileSystem fileSystem, FileStatus parent) throws IOException {
    return fileSystem.listStatus(parent.getPath());
  }

  private void traverseNonEmptyDirectory(SequenceFile.Writer fileListWriter, FileStatus sourceStatus, Path sourcePathRoot,
      DistCpOptions options) throws IOException {
    FileSystem sourceFS = sourcePathRoot.getFileSystem(getConf());
    final boolean preserveAcls = options.shouldPreserve(FileAttribute.ACL);
    final boolean preserveXAttrs = options.shouldPreserve(FileAttribute.XATTR);
    final boolean preserveRawXattrs = options.shouldPreserveRawXattrs();
    Stack<FileStatus> pathStack = new Stack<>();
    pathStack.push(sourceStatus);

    while (!pathStack.isEmpty()) {
      for (FileStatus child : getChildren(sourceFS, pathStack.pop())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Recording source-path: {} for copy.", sourceStatus.getPath());
        }
        CopyListingFileStatus childCopyListingStatus = DistCpUtils.toCopyListingFileStatus(sourceFS, child,
            preserveAcls && child.isDirectory(), preserveXAttrs && child.isDirectory(), preserveRawXattrs && child.isDirectory());
        writeToFileListing(fileListWriter, childCopyListingStatus, sourcePathRoot, options);
        if (isDirectoryAndNotEmpty(sourceFS, child)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Traversing non-empty source dir: {}", sourceStatus.getPath());
          }
          pathStack.push(child);
        }
      }
    }
  }

  private void writeToFileListingRoot(SequenceFile.Writer fileListWriter, CopyListingFileStatus fileStatus, Path sourcePathRoot,
      DistCpOptions options) throws IOException {
    boolean syncOrOverwrite = options.shouldSyncFolder() || options.shouldOverwrite();
    if (fileStatus.getPath().equals(sourcePathRoot) && fileStatus.isDirectory() && syncOrOverwrite) {
      // Skip the root-paths when syncOrOverwrite
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip {}", fileStatus.getPath());
      }
      return;
    }
    writeToFileListing(fileListWriter, fileStatus, sourcePathRoot, options);
  }

  private void writeToFileListing(SequenceFile.Writer fileListWriter, CopyListingFileStatus fileStatus, Path sourcePathRoot,
      DistCpOptions options) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("REL PATH: {}, FULL PATH: {}",
          DistCpUtils.getRelativePath(sourcePathRoot, fileStatus.getPath()), fileStatus.getPath());
    }

    if (!shouldCopy(fileStatus.getPath(), options)) {
      return;
    }

    fileListWriter.append(getFileListingKey(sourcePathRoot, fileStatus),
        getFileListingValue(fileStatus));
    fileListWriter.sync();

    if (!fileStatus.isDirectory()) {
      totalBytesToCopy += fileStatus.getLen();
    }
    totalPaths++;
  }

  /**
   * Returns the key for an entry in the copy listing sequence file
   * @param sourcePathRoot the root source path for determining the relative target path
   * @param fileStatus the copy listing file status
   * @return the key for the sequence file entry
   */
  protected Text getFileListingKey(Path sourcePathRoot, CopyListingFileStatus fileStatus) {
    Path fileStatusPath = fileStatus.getPath();
    String pathName = fileStatusPath.getName();
    String renamedPathName = pathRenames.get(pathName);

    if (renamedPathName != null && !pathName.equals(renamedPathName)) {
      LOG.info("Applying dynamic rename of {} to {}", pathName, renamedPathName);
      fileStatusPath = new Path(fileStatusPath.getParent(), renamedPathName);
    }
    return new Text(DistCpUtils.getRelativePath(sourcePathRoot, fileStatusPath));
  }

  /**
   * Returns the value for an entry in the copy listing sequence file
   * @param fileStatus the copy listing file status
   * @return the value for the sequence file entry
   */
  protected CopyListingFileStatus getFileListingValue(CopyListingFileStatus fileStatus) {
    return fileStatus;
  }

  @Override
  protected long getBytesToCopy() {
    return totalBytesToCopy;
  }

  @Override
  protected long getNumberOfPaths() {
    return totalPaths;
  }
}