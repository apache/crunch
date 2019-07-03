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
package org.apache.crunch.io.text;

import com.google.common.collect.Maps;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.plan.PlanningParameters;
import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class TextPathPerKeyTarget extends FileTargetImpl {

    private Map<String, String> extraConf = Maps.newHashMap();

    private static final Logger LOG = LoggerFactory.getLogger(TextPathPerKeyTarget.class);

    public TextPathPerKeyTarget(String path) {
        this(new Path(path));
    }

    public TextPathPerKeyTarget(Path path) {
        this(path, SequentialFileNamingScheme.getInstance());
    }

    public TextPathPerKeyTarget(Path path, FileNamingScheme fileNamingScheme) {
        super(path, TextPathPerKeyOutputFormat.class, fileNamingScheme);
    }

    @Override
    public boolean accept(OutputHandler handler, PType<?> ptype) {
        if (ptype instanceof PTableType && ptype.getFamily() == WritableTypeFamily.getInstance()) {
            if (String.class.equals(((PTableType) ptype).getKeyType().getTypeClass())) {
                handler.configure(this, ptype);
                return true;
            }
        }
        return false;
    }

    @Override
    public Target outputConf(String key, String value) {
        extraConf.put(key, value);
        return this;
    }

    @Override
    public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
        FormatBundle bundle = FormatBundle.forOutput(TextPathPerKeyOutputFormat.class);
        for (Map.Entry<String, String> e : extraConf.entrySet()) {
            bundle.set(e.getKey(), e.getValue());
        }

        Converter converter = ((PTableType) ptype).getValueType().getConverter();
        Class valueClass = converter.getValueClass();
        configureForMapReduce(job, valueClass, NullWritable.class, bundle, outputPath, name);
    }

    @Override
    public void handleOutputs(Configuration conf, Path workingPath, int index) throws IOException {
        FileSystem srcFs = workingPath.getFileSystem(conf);
        Path base = new Path(workingPath, PlanningParameters.MULTI_OUTPUT_PREFIX + index);
        if (!srcFs.exists(base)) {
            LOG.warn("Nothing to copy from {}", base);
            return;
        }

        FileSystem dstFs = path.getFileSystem(conf);
        if (!dstFs.exists(path)) {
            dstFs.mkdirs(path);
        }

        boolean sameFs = isCompatible(srcFs, path);
        move(conf, base, srcFs, path, dstFs, sameFs);
        dstFs.create(getSuccessIndicator(), true).close();
    }

    private void move(Configuration conf, Path srcBase, FileSystem srcFs, Path dstBase, FileSystem dstFs, boolean sameFs)
            throws IOException {
        Path[] keys = FileUtil.stat2Paths(srcFs.listStatus(srcBase));
        if (!dstFs.exists(dstBase)) {
            dstFs.mkdirs(dstBase);
        }
        for (Path key : keys) {
            Path[] srcs = FileUtil.stat2Paths(srcFs.listStatus(key), key);
            Path targetPath = new Path(dstBase, key.getName());
            dstFs.mkdirs(targetPath);
            for (Path s : srcs) {
                if (srcFs.isDirectory(s)) {
                    move(conf, key, srcFs, targetPath, dstFs, sameFs);
                } else {
                    Path d = getDestFile(conf, s, targetPath, s.getName().contains("-m-"));
                    if (sameFs) {
                        srcFs.rename(s, d);
                    } else {
                        FileUtil.copy(srcFs, s, dstFs, d, true, true, conf);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return "TextFilePerKey(" + path + ")";
    }
}
