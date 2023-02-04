/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.parquet.reader;

import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.cache.MetadataReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.ehcache.sizeof.SizeOf;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.facebook.presto.parquet.reader.ParquetReader.MAX_BUFFER_SIZE_DATA_SOURCE_READ;
import static com.facebook.presto.parquet.reader.TestEncryption.constructField;
import static com.facebook.presto.parquet.reader.TestEncryption.createParquetReader;
import static com.facebook.presto.parquet.reader.TestEncryption.validateColumn;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestLargeRowGroup
{
    private final Configuration conf = new Configuration(false);

    @Test
    public void testDataSourceIsReadInBoundedChunksForALargeColumnChunk()
            throws IOException
    {
        MessageType schema = new MessageType("schema", new PrimitiveType(OPTIONAL, BINARY, "col1"));
        //Create a parquet file with a columnChunk has more than 8MiB of data
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withCodec("UNCOMPRESSED")
                .withNumRecord(120000)
                .withRowGroupSize(1024 * 1024 * 1024)
                .build();

        TestParquetFileProperties parquetFile = createTestParquetFile(inputFile);
        List<BlockMetaData> rowGroupsMetadata = parquetFile.getParquetMetadata().getBlocks();
        assertEquals(rowGroupsMetadata.size(), 1, "Test requires only 1 row group to be created");

        ColumnChunkMetaData col1ColChunkMetadata = rowGroupsMetadata.get(0).getColumns().get(0);
        long totalSize = col1ColChunkMetadata.getTotalSize();
        assertTrue(totalSize > MAX_BUFFER_SIZE_DATA_SOURCE_READ, "Test setup requires a single ColumnChunk more than MAX_BUFFER_SIZE_DATA_SOURCE_READ");

        //Read all columns in the file
        validateFile(parquetFile.getParquetReader(), parquetFile.getMessageColumnIO(), inputFile);

        List<Integer> bytesFetchedPerCall = parquetFile.getDataSource().getDataSourceBytesFetchedPerCall();
        assertTrue(bytesFetchedPerCall.size() > 1, "Expected more than one call to dataSource.readFully");

        //Verify that we don't read more tha MAX_BUFFER_SIZE_DATA_SOURCE_READ in any read call from the dataSource
        //The underlying BufferedInputStream will therefore also be using at max MAX_BUFFER_SIZE_DATA_SOURCE_READ at any time
        for (Integer length : bytesFetchedPerCall) {
            assertTrue(length <= MAX_BUFFER_SIZE_DATA_SOURCE_READ);
        }
    }

    //We test that the total memory consumed to parse and read this column chunk is NOT a function of the number of data pages
    // in a ColumnChunk. Instead, we use a bounded amount of memory to read any ColumnChunk since we don't materialize the full
    // chunk in memory at any time

    @Test
    public void testMemoryUsedIsNotAFunctionOfDataPageCount()
            throws IOException
    {
        MessageType schema = new MessageType("schema", new PrimitiveType(OPTIONAL, BINARY, "col1"));

        //These memory range values will change as the implementation/memory accounting changes
        //Update them as needed
        //We allocate a buffer for reading the ColumnChunk
        long expectedMaxMemoryUsage = (long) (MAX_BUFFER_SIZE_DATA_SOURCE_READ * 1.10); //Max memory = 10% more

        SizeOf sizeOf = SizeOf.newInstance();
        for (Integer testPageCount : Arrays.asList(1000, 5000, 10000, 20000)) {
            //Create an input file with 1 row group, 1 column and a pre-determined number of data pages
            TestParquetFileProperties parquetFile = createTestParquetFile(schema, testPageCount);
            long expectedRowCount = parquetFile.getParquetMetadata().getBlocks().get(0).getRowCount();

            //Read the column completely
            Field col1 = constructField(VARCHAR, lookupColumnByName(parquetFile.getMessageColumnIO(), "col1")).orElse(null);
            long maxSystemMemoryUsed = 0;

            ParquetReader parquetReader = parquetFile.getParquetReader();
            long parquetReaderSizeBefore = sizeOf.deepSizeOf(parquetReader);
            long totalRowsRead = 0;
            while (totalRowsRead < expectedRowCount) {
                parquetReader.nextBatch();
                int rowsRead = parquetReader.readBlock(col1).getPositionCount();
                totalRowsRead += rowsRead;
                maxSystemMemoryUsed = Math.max(maxSystemMemoryUsed, parquetReader.getSystemMemoryContext().getBytes());
            }
            long parquetReaderSizeAfter = sizeOf.deepSizeOf(parquetReader);
            long parquetReaderObjectGraphSize = parquetReaderSizeAfter - parquetReaderSizeBefore;

            String testAssertFormat = "[%d] pages :: %s :: actual [%d], expected < [%d]";
            assertTrue(maxSystemMemoryUsed < expectedMaxMemoryUsage,
                    String.format(testAssertFormat, testPageCount, "maxSystemMemoryUsed", maxSystemMemoryUsed, expectedMaxMemoryUsage));
            assertTrue(parquetReaderObjectGraphSize < expectedMaxMemoryUsage,
                    String.format(testAssertFormat, testPageCount, "parquetReaderObjectGraphSize", parquetReaderObjectGraphSize, expectedMaxMemoryUsage));

            parquetReader.close();
        }
    }
    private TestParquetFileProperties createTestParquetFile(MessageType schema, int requestedPageCount)
            throws IOException
    {
        int pageSize = 100;
        TestFile inputFile = new TestFileBuilder(conf, schema)
                .withCodec("UNCOMPRESSED")
                .withPageSize(pageSize) //Keep page size small, so we get a column chunk with a lot of pages
                .withNumRecord(requestedPageCount * pageSize) //Since we're using an UNCOMPRESSED file, we can set the record count to an exact multiple of the pageSize
                //Keep row group size large, so we get can fit all pages in one row group
                .withRowGroupSize(1024 * 1024 * 1024)
                .build();

        TestParquetFileProperties parquetFile = createTestParquetFile(inputFile);
        List<BlockMetaData> rowGroupsMetadata = parquetFile.getParquetMetadata().getBlocks();
        assertEquals(rowGroupsMetadata.size(), 1, "Test requires only 1 row group to be created");

        ColumnChunkMetaData col1ColChunkMetadata = rowGroupsMetadata.get(0).getColumns().get(0);
        int actualDataPageCount = col1ColChunkMetadata.getEncodingStats().getNumDataPagesEncodedAs(Encoding.PLAIN);
        assertEquals(actualDataPageCount, requestedPageCount);

        return parquetFile;
    }

    private TestParquetFileProperties createTestParquetFile(TestFile inputFile)
            throws IOException
    {
        Path path = new Path(inputFile.getFileName());
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(path);
        long fileSize = fileSystem.getFileStatus(path).getLen();
        MockParquetDataSource dataSource = new MockParquetDataSource(new ParquetDataSourceId(path.toString()), fileSize, inputStream);
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, inputFile.getFileSize(), Optional.empty()).getParquetMetadata();
        FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
        MessageType fileSchema = fileMetaData.getSchema();
        MessageColumnIO messageColumn = getColumnIO(fileSchema, fileSchema);
        ParquetReader parquetReader = createParquetReader(parquetMetadata, messageColumn, dataSource, Optional.empty());

        return new TestParquetFileProperties(messageColumn, parquetReader, dataSource, parquetMetadata);
    }

    private static void validateFile(ParquetReader parquetReader, MessageColumnIO messageColumn, TestFile inputFile)
            throws IOException
    {
        int rowIndex = 0;
        int batchSize = parquetReader.nextBatch();
        while (batchSize > 0) {
            validateColumn("col1", VARCHAR, rowIndex, parquetReader, messageColumn, inputFile);
            rowIndex += batchSize;
            batchSize = parquetReader.nextBatch();
        }
    }
    private static class TestParquetFileProperties
    {
        private MessageColumnIO messageColumnIO;
        private ParquetReader parquetReader;
        private MockParquetDataSource dataSource;
        private ParquetMetadata parquetMetadata;

        public TestParquetFileProperties(MessageColumnIO messageColumnIO, ParquetReader parquetReader, MockParquetDataSource dataSource, ParquetMetadata parquetMetadata)
        {
            this.messageColumnIO = messageColumnIO;
            this.parquetReader = parquetReader;
            this.dataSource = dataSource;
            this.parquetMetadata = parquetMetadata;
        }

        public MessageColumnIO getMessageColumnIO()
        {
            return messageColumnIO;
        }

        public ParquetReader getParquetReader()
        {
            return parquetReader;
        }

        public MockParquetDataSource getDataSource()
        {
            return dataSource;
        }

        public ParquetMetadata getParquetMetadata()
        {
            return parquetMetadata;
        }
    }
}
