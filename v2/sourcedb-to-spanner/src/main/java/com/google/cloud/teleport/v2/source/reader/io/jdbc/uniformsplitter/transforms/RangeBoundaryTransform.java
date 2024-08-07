/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryTypeMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Transform that wraps {@link RangeBoundaryDoFn} to get to find boundary (min, max) for a column
 * (optionally for a given parent range).
 */
@AutoValue
public abstract class RangeBoundaryTransform
    extends PTransform<PCollection<ColumnForBoundaryQuery>, PCollection<Range>>
    implements Serializable {

  /** Provider for {@link DataSource}. */
  abstract SerializableFunction<Void, DataSource> dataSourceProviderFn();

  /**
   * Implementations of {@link UniformSplitterDBAdapter} to get queries as per the dialect of the
   * database.
   */
  abstract UniformSplitterDBAdapter dbAdapter();

  /** Name of the table. */
  abstract String tableName();

  /** Partition Columns. */
  abstract ImmutableList<String> partitionColumns();

  /** Type mapper to help map types like {@link String String.Class}. */
  @Nullable
  abstract BoundaryTypeMapper boundaryTypeMapper();

  @Override
  public PCollection<Range> expand(PCollection<ColumnForBoundaryQuery> input) {
    return input.apply(
        ParDo.of(
            new RangeBoundaryDoFn(
                dataSourceProviderFn(),
                dbAdapter(),
                tableName(),
                partitionColumns(),
                boundaryTypeMapper())));
  }

  public static Builder builder() {
    return new AutoValue_RangeBoundaryTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDataSourceProviderFn(SerializableFunction<Void, DataSource> value);

    public abstract Builder setDbAdapter(UniformSplitterDBAdapter value);

    public abstract Builder setTableName(String value);

    public abstract Builder setPartitionColumns(ImmutableList<String> value);

    public abstract Builder setBoundaryTypeMapper(@Nullable BoundaryTypeMapper value);

    public abstract RangeBoundaryTransform build();
  }
}
