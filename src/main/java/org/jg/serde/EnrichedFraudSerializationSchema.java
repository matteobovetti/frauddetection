package org.jg.serde;

import org.apache.flink.types.RowKind;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.jg.entity.EnrichedFraud;

public class EnrichedFraudSerializationSchema implements FlussSerializationSchema<EnrichedFraud> {

  private static final long serialVersionUID = 1L;

  @Override
  public void open(InitializationContext context) throws Exception {}

  @Override
  public RowWithOp serialize(EnrichedFraud value) throws Exception {
    GenericRow row = new GenericRow(3);
    row.setField(0, value.getTransactionId());
    row.setField(1, value.getAccountId());
    row.setField(2, BinaryString.fromString(value.getName()));

    RowKind rowKind = value.getKind();
    switch (rowKind) {
      case INSERT:
        return new RowWithOp(row, OperationType.APPEND);
      default:
        throw new IllegalArgumentException("Unsupported row kind: " + rowKind);
    }
  }
}
