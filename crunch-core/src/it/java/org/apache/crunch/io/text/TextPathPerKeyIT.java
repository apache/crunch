/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.io.text;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.Compress;
import org.apache.crunch.io.From;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TextPathPerKeyIT extends CrunchTestSupport implements Serializable {
    @Test
    public void testOutputFilePerKey() throws Exception {
        Pipeline p = new MRPipeline(this.getClass(), tempDir.getDefaultConfiguration());
        Path outDir = tempDir.getPath("out");
        PTable<String, String> pTable = p.read(From.textFile(tempDir.copyResourceFileName("docs.txt")))
                .parallelDo(new MapFn<String, Pair<String, String>>() {
                    @Override
                    public Pair<String, String> map(String input) {
                        String[] p = input.split("\t");
                        return Pair.of(p[0], p[1]);
                    }
                }, Writables.tableOf(Writables.strings(), Writables.strings()));

        pTable.groupByKey()
                .write(new TextPathPerKeyTarget(outDir));
        p.done();

        Set<String> names = Sets.newHashSet();
        FileSystem fs = outDir.getFileSystem(tempDir.getDefaultConfiguration());
        for (FileStatus fstat : fs.listStatus(outDir)) {
            names.add(fstat.getPath().getName());
        }
        assertEquals(ImmutableSet.of("A", "B", "_SUCCESS"), names);

        FileStatus[] aStat = fs.listStatus(new Path(outDir, "A"));
        assertEquals(1, aStat.length);
        assertEquals("part-r-00000", aStat[0].getPath().getName());

        FileStatus[] bStat = fs.listStatus(new Path(outDir, "B"));
        assertEquals(1, bStat.length);
        assertEquals("part-r-00000", bStat[0].getPath().getName());
    }

    @Test
    public void testOutputFilePerKeyMapOnlyJob() throws Exception {
        Pipeline p = new MRPipeline(this.getClass(), tempDir.getDefaultConfiguration());
        Path outDir = tempDir.getPath("out");
        PTable<String, String> pTable = p.read(From.textFile(tempDir.copyResourceFileName("docs.txt")))
                .parallelDo(new MapFn<String, Pair<String, String>>() {
                    @Override
                    public Pair<String, String> map(String input) {
                        String[] p = input.split("\t");
                        return Pair.of(p[0], p[1]);
                    }
                }, Writables.tableOf(Writables.strings(), Writables.strings()));

        pTable.write(new TextPathPerKeyTarget(outDir));
        p.done();

        Set<String> names = Sets.newHashSet();
        FileSystem fs = outDir.getFileSystem(tempDir.getDefaultConfiguration());
        for (FileStatus fstat : fs.listStatus(outDir)) {
            names.add(fstat.getPath().getName());
        }
        assertEquals(ImmutableSet.of("A", "B", "_SUCCESS"), names);

        FileStatus[] aStat = fs.listStatus(new Path(outDir, "A"));
        assertEquals(1, aStat.length);
        assertEquals("part-m-00000", aStat[0].getPath().getName());

        FileStatus[] bStat = fs.listStatus(new Path(outDir, "B"));
        assertEquals(1, bStat.length);
        assertEquals("part-m-00000", bStat[0].getPath().getName());
    }

    @Test
    public void testOutputFilePerKeyWithNestedSubFoldersAndCompression() throws Exception {
        Pipeline p = new MRPipeline(this.getClass(), tempDir.getDefaultConfiguration());
        Path outDir = tempDir.getPath("out");
        PTable<String, String> pTable = p.read(From.textFile(tempDir.copyResourceFileName("docs.txt")))
                .parallelDo(new MapFn<String, Pair<String, String>>() {
                    @Override
                    public Pair<String, String> map(String input) {
                        String[] p = input.split("\t");
                        return Pair.of(p[0] + "/dir", p[1]);
                    }
                }, Writables.tableOf(Writables.strings(), Writables.strings()));

        pTable.groupByKey()
                .write(new TextPathPerKeyTarget(outDir));

        Path outDir2 = tempDir.getPath("out2");

        pTable.groupByKey()
                .write(Compress.gzip(new TextPathPerKeyTarget(outDir2)));
        p.done();

        assertSubDirs(outDir, "");
        assertSubDirs(outDir2, ".gz");
    }

    private void assertSubDirs(Path outDir, String extension) throws IOException {
        Set<String> names = Sets.newHashSet();
        FileSystem fs = outDir.getFileSystem(tempDir.getDefaultConfiguration());
        for (FileStatus fstat : fs.listStatus(outDir)) {
            names.add(fstat.getPath().getName());
        }
        assertEquals(ImmutableSet.of("A", "B", "_SUCCESS"), names);

        FileStatus[] aStat = fs.listStatus(new Path(outDir, "A"));
        assertEquals(1, aStat.length);
        assertEquals("dir", aStat[0].getPath().getName());
        FileStatus[] aDirStat = fs.listStatus(aStat[0].getPath());
        assertEquals("part-r-00000" + extension, aDirStat[0].getPath().getName());

        FileStatus[] bStat = fs.listStatus(new Path(outDir, "B"));
        assertEquals(1, bStat.length);
        assertEquals("dir", bStat[0].getPath().getName());
        FileStatus[] bDirStat = fs.listStatus(bStat[0].getPath());
        assertEquals("part-r-00000" + extension, bDirStat[0].getPath().getName());
    }

    @Test
    public void testOutputFilePerKey_NothingToOutput() throws Exception {
        Pipeline p = new MRPipeline(this.getClass(), tempDir.getDefaultConfiguration());
        Path outDir = tempDir.getPath("out");

        p.read(From.textFile(tempDir.copyResourceFileName("docs.txt")))
                .parallelDo(new MapFn<String, Pair<String, String>>() {
                    @Override
                    public Pair<String, String> map(String input) {
                        String[] p = input.split("\t");
                        return Pair.of(p[0], p[1]);
                    }
                }, Writables.tableOf(Writables.strings(), Writables.strings()))
                .filter(FilterFns.<Pair<String, String>>REJECT_ALL())
                .groupByKey()
                .write(new TextPathPerKeyTarget(outDir));
        p.done();

        FileSystem fs = outDir.getFileSystem(tempDir.getDefaultConfiguration());
        assertFalse(fs.exists(outDir));
    }

    @Test
    public void testOutputFilePerKey_Directories() throws Exception {
        Pipeline p = new MRPipeline(this.getClass(), tempDir.getDefaultConfiguration());
        Path outDir = tempDir.getPath("out");
        p.read(From.textFile(tempDir.copyResourceFileName("docs.txt")))
                .parallelDo(new MapFn<String, Pair<String, String>>() {
                    @Override
                    public Pair<String, String> map(String input) {
                        String[] p = input.split("\t");
                        return Pair.of(p[0] + "/child", p[1]);
                    }
                }, Writables.tableOf(Writables.strings(), Writables.strings()))
                .groupByKey()
                .write(new TextPathPerKeyTarget(outDir));
        p.done();

        Set<String> names = Sets.newHashSet();
        FileSystem fs = outDir.getFileSystem(tempDir.getDefaultConfiguration());
        for (FileStatus fstat : fs.listStatus(outDir)) {
            names.add(fstat.getPath().getName());
        }
        assertEquals(ImmutableSet.of("A", "B", "_SUCCESS"), names);

        Path aParent = new Path(outDir, "A");
        FileStatus[] aParentStat = fs.listStatus(aParent);
        assertEquals(1, aParentStat.length);
        assertEquals("child", aParentStat[0].getPath().getName());
        FileStatus[] aChildStat = fs.listStatus(new Path(aParent, "child"));
        assertEquals(1, aChildStat.length);
        assertEquals("part-r-00000", aChildStat[0].getPath().getName());

        Path bParent = new Path(outDir, "B");
        FileStatus[] bParentStat = fs.listStatus(bParent);
        assertEquals(1, bParentStat.length);
        assertEquals("child", bParentStat[0].getPath().getName());
        FileStatus[] bChildStat = fs.listStatus(new Path(bParent, "child"));
        assertEquals(1, bChildStat.length);
        assertEquals("part-r-00000", bChildStat[0].getPath().getName());
    }
}
