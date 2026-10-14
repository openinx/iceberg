/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.source.reader;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestWatermarkExtractorRecordEmitter {
  @TempDir protected Path temporaryFolder;

  @Test
  public void testWatermarkIsDecrementedByOne() throws IOException {
    long extractedWatermark = 1000L;
    IcebergSourceSplit split = createSplit(1L);

    WatermarkExtractorRecordEmitter<String> emitter =
        new WatermarkExtractorRecordEmitter<>(s -> extractedWatermark);

    CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
    emitter.emitRecord(new RecordAndPosition<>("record", 0, 0L), output, split);

    assertThat(output.watermarks).hasSize(1);
    assertThat(output.watermarks.get(0).getTimestamp()).isEqualTo(extractedWatermark - 1);
  }

  @Test
  public void testWatermarkEmittedOnlyOncePerSplit() throws IOException {
    IcebergSourceSplit split = createSplit(1L);

    WatermarkExtractorRecordEmitter<String> emitter =
        new WatermarkExtractorRecordEmitter<>(s -> 1000L);

    CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
    RecordAndPosition<String> element = new RecordAndPosition<>("record", 0, 0L);
    emitter.emitRecord(element, output, split);
    emitter.emitRecord(element, output, split);
    emitter.emitRecord(element, output, split);

    assertThat(output.watermarks).hasSize(1);
    assertThat(output.records).hasSize(3);
  }

  @Test
  public void testWatermarkNotEmittedWhenNewSplitHasLowerValue() throws IOException {
    IcebergSourceSplit split1 = createSplit(1L);
    IcebergSourceSplit split2 = createSplit(2L);

    Map<String, Long> watermarkMap = Maps.newHashMap();
    watermarkMap.put(split1.splitId(), 2000L);
    watermarkMap.put(split2.splitId(), 1000L);

    WatermarkExtractorRecordEmitter<String> emitter =
        new WatermarkExtractorRecordEmitter<>(s -> watermarkMap.get(s.splitId()));

    CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
    RecordAndPosition<String> element = new RecordAndPosition<>("record", 0, 0L);
    emitter.emitRecord(element, output, split1);
    emitter.emitRecord(element, output, split2);

    // Only split1's watermark is emitted; split2 has a lower value so it's skipped
    assertThat(output.watermarks).hasSize(1);
    assertThat(output.watermarks.get(0).getTimestamp()).isEqualTo(1999L);
  }

  @Test
  public void testWatermarkEmittedForEachHigherSplit() throws IOException {
    IcebergSourceSplit split1 = createSplit(1L);
    IcebergSourceSplit split2 = createSplit(2L);

    Map<String, Long> watermarkMap = Maps.newHashMap();
    watermarkMap.put(split1.splitId(), 1000L);
    watermarkMap.put(split2.splitId(), 2000L);

    WatermarkExtractorRecordEmitter<String> emitter =
        new WatermarkExtractorRecordEmitter<>(s -> watermarkMap.get(s.splitId()));

    CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
    RecordAndPosition<String> element = new RecordAndPosition<>("record", 0, 0L);
    emitter.emitRecord(element, output, split1);
    emitter.emitRecord(element, output, split2);

    assertThat(output.watermarks).hasSize(2);
    assertThat(output.watermarks.get(0).getTimestamp()).isEqualTo(999L);
    assertThat(output.watermarks.get(1).getTimestamp()).isEqualTo(1999L);
  }

  @Test
  public void testWatermarkAtLongMinValueDoesNotOverflow() throws IOException {
    IcebergSourceSplit split = createSplit(1L);

    WatermarkExtractorRecordEmitter<String> emitter =
        new WatermarkExtractorRecordEmitter<>(s -> Long.MIN_VALUE);

    CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
    emitter.emitRecord(new RecordAndPosition<>("record", 0, 0L), output, split);

    assertThat(output.watermarks).hasSize(1);
    assertThat(output.watermarks.get(0).getTimestamp()).isEqualTo(Long.MIN_VALUE);
  }

  @ParameterizedTest
  @CsvSource({
    "6, -1001000",
    "6, -1000000",
    "6, -1000",
    "6, 0",
    "6, 1000",
    "6, 1000000",
    "6, 1001000",
    "9, -1000001",
    "9, -1000000",
    "9, -1",
    "9, 0",
    "9, 1",
    "9, 1000000",
    "9, 1000001"
  })
  void timestampWatermarkPrecedesFirstRecord(int precision, long nanosFromEpoch)
      throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "event_time",
                precision == 6
                    ? Types.TimestampType.withoutZone()
                    : Types.TimestampNanoType.withoutZone()));
    Record record = GenericRecord.create(schema);
    record.setField(
        "event_time",
        LocalDateTime.ofInstant(Instant.ofEpochSecond(0, nanosFromEpoch), ZoneOffset.UTC));
    IcebergSourceSplit split =
        IcebergSourceSplit.fromCombinedScanTask(
            ReaderUtil.createCombinedScanTask(
                List.of(List.of(record)), temporaryFolder, FileFormat.PARQUET, schema));
    WatermarkExtractorRecordEmitter<RowData> emitter =
        new WatermarkExtractorRecordEmitter<>(
            new ColumnStatsWatermarkExtractor(schema, "event_time", null));
    CapturingSourceOutput<RowData> output =
        new CapturingSourceOutput<>() {
          @Override
          public void collect(RowData row) {
            assertThat(watermarks)
                .singleElement()
                .satisfies(
                    watermark ->
                        assertThat(watermark.getTimestamp())
                            .isEqualTo(row.getTimestamp(0, precision).getMillisecond() - 1));
            super.collect(row);
          }
        };

    try (DataIterator<RowData> records =
        ReaderUtil.createDataIterator(split.task(), schema, schema)) {
      emitter.emitRecord(new RecordAndPosition<>(records.next(), 0, 1L), output, split);
    }

    assertThat(output.records).hasSize(1);
  }

  private IcebergSourceSplit createSplit(long seed) throws IOException {
    List<List<Record>> recordBatchList =
        ReaderUtil.createRecordBatchList(seed, TestFixtures.SCHEMA, 1, 1);
    return IcebergSourceSplit.fromCombinedScanTask(
        ReaderUtil.createCombinedScanTask(
            recordBatchList, temporaryFolder, FileFormat.PARQUET, TestFixtures.SCHEMA));
  }

  private static class CapturingSourceOutput<T> implements SourceOutput<T> {
    final List<Watermark> watermarks = Lists.newArrayList();
    final List<T> records = Lists.newArrayList();

    @Override
    public void collect(T record) {
      records.add(record);
    }

    @Override
    public void collect(T record, long timestamp) {
      records.add(record);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      watermarks.add(watermark);
    }

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}
  }
}
