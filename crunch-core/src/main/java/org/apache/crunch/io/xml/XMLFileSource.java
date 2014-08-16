package org.apache.crunch.io.xml;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.crunch.ReadableData;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * A {@code Source} instance that uses the {@code XMLInputFormat}, which feeds
 * each map task one single XML record at a time.
 */
public class XMLFileSource extends FileSourceImpl<String> implements ReadableSource<String> {
  /**
   * The key used in the {@code XMLInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code XMLReader}'s buffer size
   */
  public static final String BUFFER_SIZE = "xml.buffersize";

  /**
   * The key used in the {@code XMLInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code XMLReader}'s input file encoding
   */
  public static final String INPUT_FILE_ENCODING = "xml.inputfileencoding";

  /**
   * The key used in the {@code XMLInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code XMLReader}'s maximum record size. If this is not set,
   * INPUT_SPLIT_SIZE will be checked first, and if that is not set, 64mb will
   * be assumed.
   */
  public static final String MAXIMUM_RECORD_SIZE = "xml.maximumrecordsize";

  /**
   * The key used in the {@code XMLInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code XMLReader}'s input split size. If it is not set, 64mb
   * will be assumed.
   */
  public static final String INPUT_SPLIT_SIZE = "xml.inputsplitsize";

  /**
   * The key used in the {@code XMLInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code XMLReader}'s open tag. If it is not set, an exception
   * will be thrown
   */
  public static final String OPEN_PATTERN = "xml.openpattern";

  /**
   * The key used in the {@code XMLInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code XMLReader}'s close tag. If it is not set, an
   * exception will be thrown
   */
  public static final String CLOSE_PATTERN = "xml.closepattern";

  private String inputFileEncoding;
  private int maximumRecordSize;
  private int bufferSize;
  private Pattern openPattern;
  private Pattern closePattern;

  /**
   * Creates a new XMLFileSource instance
   * 
   * @param path
   *          The {@code Path} to the input data
   * @param openPattern
   *          A regular expression representing the start of a record.
   * @param closePattern
   *          A regular expression representing the end of a record.
   */
  public XMLFileSource(final Path path, final Pattern openPattern, final Pattern closePattern) {
    this(path, XMLReader.DEFAULT_BUFFER_SIZE, XMLReader.DEFAULT_INPUT_FILE_ENCODING, openPattern, closePattern,
        XMLReader.DEFAULT_MAXIMUM_RECORD_SIZE);
  }

  /**
   * Creates a new XMLFileSource instance
   * 
   * @param paths
   *          A list of {@code Path}s to be used as input data.
   * @param openPattern
   *          A regular expression representing the start of a record.
   * @param closePattern
   *          A regular expression representing the end of a record.
   */
  public XMLFileSource(final List<Path> paths, final Pattern openPattern, final Pattern closePattern) {
    this(paths, XMLReader.DEFAULT_BUFFER_SIZE, XMLReader.DEFAULT_INPUT_FILE_ENCODING, openPattern, closePattern,
        XMLReader.DEFAULT_MAXIMUM_RECORD_SIZE);
  }

  /**
   * Creates a new XMLFileSource instance
   * 
   * @param path
   *          The {@code Path} to the input data
   * @param bufferSize
   *          The size of the buffer to be used in the underlying
   *          {@code XMLReader}
   * @param inputFileEncoding
   *          The the encoding of the input file to be read by the underlying
   *          {@code XMLReader}
   * @param openPattern
   *          A regular expression representing the start of a record.
   * @param closePattern
   *          A regular expression representing the end of a record.
   * @param maximumRecordSize
   *          The maximum acceptable size of one XML record. Beyond this limit,
   *          {@code XMLReader} will stop parsing and an exception will be
   *          thrown.
   */
  public XMLFileSource(final Path path, final int bufferSize, final String inputFileEncoding,
      final Pattern openPattern, final Pattern closePattern, final int maximumRecordSize) {
    super(path, Writables.strings(), getXMLBundle(bufferSize, inputFileEncoding, openPattern, closePattern,
        maximumRecordSize, true));
    setPrivateVariables(bufferSize, inputFileEncoding, openPattern, closePattern, maximumRecordSize);
  }

  /**
   * Creates a new XMLFileSource instance
   * 
   * @param paths
   *          A list of {@code Path}s to be used as input data.
   * @param bufferSize
   *          The size of the buffer to be used in the underlying
   *          {@code XMLReader}
   * @param inputFileEncoding
   *          The the encoding of the input file to be read by the underlying
   *          {@code XMLReader}
   * @param openPattern
   *          A regular expression representing the start of a record.
   * @param closePattern
   *          A regular expression representing the end of a record.
   * @param maximumRecordSize
   *          The maximum acceptable size of one XML record. Beyond this limit,
   *          {@code XMLReader} will stop parsing and an exception will be
   *          thrown.
   */
  public XMLFileSource(final List<Path> paths, final int bufferSize, final String inputFileEncoding,
      final Pattern openPattern, final Pattern closePattern, final int maximumRecordSize) {
    super(paths, Writables.strings(), getXMLBundle(bufferSize, inputFileEncoding, openPattern, closePattern,
        maximumRecordSize, true));
    setPrivateVariables(bufferSize, inputFileEncoding, openPattern, closePattern, maximumRecordSize);
  }

  @Override
  public Iterable<String> read(final Configuration conf) throws IOException {
    return read(conf, new XMLFileReaderFactory(openPattern, closePattern, bufferSize, inputFileEncoding,
        maximumRecordSize));
  }

  @Override
  public ReadableData<String> asReadable() {
    return new XMLReadableData(paths, bufferSize, inputFileEncoding, openPattern, closePattern, maximumRecordSize);
  }

  /**
   * Configures the job with any custom options. These will be retrieved later
   * by {@code XMLInputFormat}
   */
  private static FormatBundle<XMLInputFormat> getXMLBundle(final int bufferSize, final String inputFileEncoding,
      final Pattern openPattern, final Pattern closePattern, final int maximumRecordSize, final boolean useRegex) {
    final FormatBundle<XMLInputFormat> bundle = FormatBundle.forInput(XMLInputFormat.class);
    bundle.set(RuntimeParameters.DISABLE_COMBINE_FILE, "true");
    bundle.set(BUFFER_SIZE, String.valueOf(bufferSize));
    bundle.set(INPUT_FILE_ENCODING, inputFileEncoding);
    bundle.set(OPEN_PATTERN, openPattern.pattern());
    bundle.set(CLOSE_PATTERN, closePattern.pattern());
    bundle.set(MAXIMUM_RECORD_SIZE, String.valueOf(maximumRecordSize));
    return bundle;
  }

  private void setPrivateVariables(final int bufferSize, final String inputFileEncoding, final Pattern openPattern,
      final Pattern closePattern, final int maximumRecordSize) {
    this.bufferSize = bufferSize;
    this.inputFileEncoding = inputFileEncoding;
    this.maximumRecordSize = maximumRecordSize;
    this.openPattern = openPattern;
    this.closePattern = closePattern;
  }

  @Override
  public String toString() {
    return "XML(" + pathsAsString() + ")";
  }
}
