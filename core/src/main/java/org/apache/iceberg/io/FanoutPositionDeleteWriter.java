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

package org.apache.iceberg.io;

import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;

public class FanoutPositionDeleteWriter<T> extends FanoutWriter<PositionDelete<T>, DeleteWriteResult> {

  private final FileWriterFactory<T> writerFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final FileFormat fileFormat;
  private final long targetFileSizeBytes;
  private final List<DeleteFile> deleteFiles;
  private final CharSequenceSet referencedDataFiles;

  public FanoutPositionDeleteWriter(
      FileWriterFactory<T> writerFactory, OutputFileFactory fileFactory,
      FileIO io, FileFormat fileFormat, long targetFileSizeBytes) {
    this.writerFactory = writerFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.fileFormat = fileFormat;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.deleteFiles = Lists.newArrayList();
    this.referencedDataFiles = CharSequenceSet.empty();
  }

  @Override
  protected FileWriter<PositionDelete<T>, DeleteWriteResult> newWriter(PartitionSpec spec, StructLike partition) {
    // TODO: support ORC rolling writers.
    if (fileFormat == FileFormat.ORC) {
      EncryptedOutputFile outputFile = newOutputFile(fileFactory, spec, partition);
      return writerFactory.newPositionDeleteWriter(outputFile, spec, partition);
    } else {
      return new RollingPositionDeleteWriter<>(writerFactory, fileFactory, io, targetFileSizeBytes, spec, partition);
    }
  }

  @Override
  protected void addResult(DeleteWriteResult result) {
    deleteFiles.addAll(result.deleteFiles());
    referencedDataFiles.addAll(result.referencedDataFiles());
  }

  @Override
  protected DeleteWriteResult aggregatedResult() {
    return new DeleteWriteResult(deleteFiles, referencedDataFiles);
  }
}
