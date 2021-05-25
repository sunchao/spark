/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

class ParquetReadState {
  private final ParquetReadInfo columnInfo;
  private final WritableColumnVector vector;
  private final List<ParquetReadState> children;

  /**
   * repetition & definition levels
   * these are allocated only for leaf states; for non-leaf states, they simply maintain
   * references to that of the former.
   */
  private WritableColumnVector repetitionLevels;
  private WritableColumnVector definitionLevels;

  /** whether this column is primitive (i.e., leaf column) */
  private final boolean isPrimitive;

  /** reader for this column - only set if 'isPrimitive' is true */
  private VectorizedColumnReader columnReader;

  ParquetReadState(
      ParquetReadInfo columnInfo,
      WritableColumnVector vector,
      int capacity,
      MemoryMode memoryMode) {
    if (!columnInfo.sparkType().sameType(vector.dataType())) {
      throw new IllegalArgumentException("Spark type: " + columnInfo.sparkType() +
          " doesn't match the type: " + vector.dataType() + " in column vector");
    }
    this.columnInfo = columnInfo;
    this.vector = vector;
    this.children = new ArrayList<>();
    this.isPrimitive = columnInfo.isPrimitive();

    if (isPrimitive) {
      repetitionLevels = allocateLevelsVector(capacity, memoryMode);
      definitionLevels = allocateLevelsVector(capacity, memoryMode);
    } else {
      DataType type = columnInfo.sparkType();
      ParquetGroupReadInfo groupInfo = (ParquetGroupReadInfo) columnInfo;
      if (type instanceof ArrayType) {
        ParquetReadState childState = new ParquetReadState(groupInfo.children().apply(0),
            vector.getChild(0), capacity, memoryMode);
        this.repetitionLevels = childState.repetitionLevels;
        this.definitionLevels = childState.definitionLevels;
        children.add(childState);
      } else if (type instanceof MapType) {
        ParquetReadState childState = new ParquetReadState(groupInfo.children().apply(0),
            vector.getChild(0), capacity, memoryMode);
        this.repetitionLevels = childState.repetitionLevels;
        this.definitionLevels = childState.definitionLevels;
        children.add(childState);
        children.add(new ParquetReadState(groupInfo.children().apply(1), vector.getChild(1),
            capacity, memoryMode));
      } else if (type instanceof StructType) {
        for (int i = 0; i < groupInfo.children().length(); i++) {
          ParquetReadState childState = new ParquetReadState(groupInfo.children().apply(i),
              vector.getChild(i), capacity, memoryMode);
          this.repetitionLevels = childState.repetitionLevels;
          this.definitionLevels = childState.definitionLevels;
          children.add(childState);
        }
      }
    }
  }

  public ParquetReadInfo getColumnInfo() {
    return this.columnInfo;
  }

  public WritableColumnVector getValueVector() {
    return this.vector;
  }

  public WritableColumnVector getRepetitionLevelVector() {
    return this.repetitionLevels;
  }

  public WritableColumnVector getDefinitionLevelVector() {
    return this.definitionLevels;
  }

  public VectorizedColumnReader getColumnReader() {
    return this.columnReader;
  }

  public void setColumnReader(VectorizedColumnReader reader) {
    if (!isPrimitive) {
      throw new IllegalStateException("[BUG] can't set reader for non-primitive column");
    }
    this.columnReader = reader;
  }

  /**
   * Get all the leaf states in depth-first order.
   */
  public List<ParquetReadState> getLeaves() {
    List<ParquetReadState> result = new ArrayList<>();
    getLeavesHelper(this, result);
    return result;
  }

  public void finish() {
    DataType type = columnInfo.sparkType();
    if (type instanceof ArrayType || type instanceof MapType) {
      for (ParquetReadState child : children) {
        child.finish();
      }
      calculateCollectionOffsets();
    } else if (type instanceof StructType) {
      for (ParquetReadState child : children) {
        child.finish();
      }
      calculateStructOffsets();
    }
  }

  private void calculateCollectionOffsets() {
    int maxDefinitionLevel = columnInfo.definitionLevel();
    int maxElementRepetitionLevel = columnInfo.repetitionLevel() + 1;

    // i is the index over all leaf elements of this array, while offset is the index over
    // all top elements of this array.
    for (int i = 0, offset = 0, rowId = 0; i < definitionLevels.numValues();
         i = getNextCollectionStart(maxElementRepetitionLevel, i)) {
      int definitionLevel = definitionLevels.getInt(i);
      if (definitionLevel == maxDefinitionLevel - 1) {
        // the collection is null
        vector.putNull(rowId);
      } else if (definitionLevel == maxDefinitionLevel) {
        // collection is defined but empty
        vector.putNotNull(rowId);
        vector.putArray(rowId, offset, 0);
      } else {
        // collection is defined and non-empty: find out how many top element there is till the
        // start of the next array.
        vector.putNotNull(rowId);
        int length = getCollectionSize(maxElementRepetitionLevel, i + 1);
        vector.putArray(rowId, offset, length);
        offset += length;
      }
      rowId++;
    }
  }

  private void calculateStructOffsets() {
    int maxDefinitionLevel = columnInfo.definitionLevel();
    for (int i = 0, rowId = 0; i < definitionLevels.numValues(); i++) {
      if (definitionLevels.getInt(i) == maxDefinitionLevel - 1) {
        // the struct is null
        vector.putNull(rowId);
      } else {
        vector.putNotNull(rowId);
      }
      rowId++;
    }
  }

  private static void getLeavesHelper(ParquetReadState state, List<ParquetReadState> coll) {
    if (state.isPrimitive) {
      coll.add(state);
    } else {
      for (ParquetReadState childState: state.children) {
        getLeavesHelper(childState, coll);
      }
    }
  }

  private static WritableColumnVector allocateLevelsVector(int capacity, MemoryMode memoryMode) {
    switch (memoryMode) {
      case ON_HEAP:
        return new OnHeapColumnVector(capacity, DataTypes.IntegerType);
      case OFF_HEAP:
        return new OffHeapColumnVector(capacity, DataTypes.IntegerType);
      default:
        throw new IllegalArgumentException("Unknown memory mode: " + memoryMode);
    }
  }

 private int getNextCollectionStart(int maxRepetitionLevel, int elementIndex) {
    int idx = elementIndex + 1;
    for (; idx < repetitionLevels.numValues(); idx++) {
      if (repetitionLevels.getInt(idx) < maxRepetitionLevel) {
        break;
      }
    }
    return idx;
  }

  private int getCollectionSize(int maxRepetitionLevel, int idx) {
    int size = 1;
    for (; idx < repetitionLevels.numValues(); idx++) {
      if (repetitionLevels.getInt(idx) < maxRepetitionLevel) {
        break;
      } else {
        size++;
      }
    }
    return size;
  }

}
