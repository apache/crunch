package com.cloudera.crunch;

import static com.cloudera.crunch.io.At.sequenceFile;
import static com.cloudera.crunch.io.At.textFile;
import static com.cloudera.crunch.types.writable.Writables.strings;
import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.crunch.FilterFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mem.MemPipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.test.FileHelper;

public class PCollectionGetSizeTest {

    private String emptyInputPath;
    private String nonEmptyInputPath;
    private String outputPath;

    /** Filter that rejects everything. */
    @SuppressWarnings("serial")
    private static class FalseFilterFn extends FilterFn<String> {

        @Override
        public boolean accept(final String input) {
            return false;
        }
    }

    @Before
    public void setUp() throws IOException {
        emptyInputPath = FileHelper.createTempCopyOf("emptyTextFile.txt");
        nonEmptyInputPath = FileHelper.createTempCopyOf("set1.txt");
        outputPath = FileHelper.createOutputPath().getAbsolutePath();
    }

    @Test
    public void testGetSizeOfEmptyInput_MRPipeline() throws IOException {
        testCollectionGetSizeOfEmptyInput(new MRPipeline(this.getClass()));
    }

    @Test
    public void testGetSizeOfEmptyInput_MemPipeline() throws IOException {
        testCollectionGetSizeOfEmptyInput(MemPipeline.getInstance());
    }

    private void testCollectionGetSizeOfEmptyInput(Pipeline pipeline) throws IOException {

        assertThat(pipeline.read(textFile(emptyInputPath)).getSize(), is(0L));
    }

    @Test
    public void testMaterializeEmptyInput_MRPipeline() throws IOException {
        testMaterializeEmptyInput(new MRPipeline(this.getClass()));
    }

    @Test
    public void testMaterializeEmptyImput_MemPipeline() throws IOException {
        testMaterializeEmptyInput(MemPipeline.getInstance());
    }

    private void testMaterializeEmptyInput(Pipeline pipeline) throws IOException {
        assertThat(newArrayList(pipeline.readTextFile(emptyInputPath).materialize().iterator()).size(), is(0));
    }

    @Test
    public void testGetSizeOfEmptyIntermediatePCollection_MRPipeline() throws IOException {

        PCollection<String> emptyIntermediate = createPesistentEmptyIntermediate(new MRPipeline(this.getClass()));

        assertThat(emptyIntermediate.getSize(), is(0L));
    }

    @Test
    @Ignore("GetSize of a DoCollection is only an estimate based on scale factor, so we can't count on it being reported as 0")
    public void testGetSizeOfEmptyIntermediatePCollection_NoSave_MRPipeline() throws IOException {

        PCollection<String> data = new MRPipeline(this.getClass()).readTextFile(nonEmptyInputPath);

        PCollection<String> emptyPCollection = data.filter(new FalseFilterFn());

        assertThat(emptyPCollection.getSize(), is(0L));
    }

    @Test
    @Ignore("MemPipeline implementation is inconsistent with the MRPipeline")
    public void testGetSizeOfEmptyIntermediatePCollection_MemPipeline() {

        PCollection<String> emptyIntermediate = createPesistentEmptyIntermediate(MemPipeline.getInstance());

        assertThat(emptyIntermediate.getSize(), is(0L));
    }

    @Test
    public void testMaterializeOfEmptyIntermediatePCollection_MRPipeline() throws IOException {

        PCollection<String> emptyIntermediate = createPesistentEmptyIntermediate(new MRPipeline(this.getClass()));

        assertThat(newArrayList(emptyIntermediate.materialize()).size(), is(0));
    }

    @Test
    @Ignore("MemPipelien impelmentation is inconsistent with the MRPipelien")
    public void testMaterializeOfEmptyIntermediatePCollection_MemPipeline() {

        PCollection<String> emptyIntermediate = createPesistentEmptyIntermediate(MemPipeline.getInstance());

        assertThat(newArrayList(emptyIntermediate.materialize()).size(), is(0));
    }

    private PCollection<String> createPesistentEmptyIntermediate(Pipeline pipeline) {

        PCollection<String> data = pipeline.readTextFile(nonEmptyInputPath);

        PCollection<String> emptyPCollection = data.filter(new FalseFilterFn());

        emptyPCollection.write(sequenceFile(outputPath, strings()));

        pipeline.run();

        return pipeline.read(sequenceFile(outputPath, strings()));
    }

    @Test(expected = IllegalStateException.class)
    public void testExpectExceptionForGettingSizeOfNonExistingFile_MRPipeline() throws IOException {
        new MRPipeline(this.getClass()).readTextFile("non_existing.file").getSize();
    }

    @Test(expected = IllegalStateException.class)
    @Ignore("MemPipelien impelmentation is inconsistent with the MRPipelien")
    public void testExpectExceptionForGettingSizeOfNonExistingFile_MemPipeline() {
        MemPipeline.getInstance().readTextFile("non_existing.file").getSize();
    }
}
