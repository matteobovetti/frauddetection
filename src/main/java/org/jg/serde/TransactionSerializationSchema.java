package org.jg.serde;

import org.apache.flink.types.RowKind;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.jg.entity.Transaction;

public class TransactionSerializationSchema implements FlussSerializationSchema<Transaction> {

  private static final long serialVersionUID = 1L;

  @Override
  public void open(InitializationContext context) throws Exception {}

  @Override
  public RowWithOp serialize(Transaction value) throws Exception {
    GenericRow row = new GenericRow(4);
    row.setField(0, value.getId());
    row.setField(1, value.getAccountId());
    row.setField(2, value.getCreatedAt());
    row.setField(3, Decimal.fromBigDecimal(value.getAmount(), 10, 2));

    RowKind rowKind = value.getKind();
    switch (rowKind) {
      case INSERT:
        return new RowWithOp(row, OperationType.APPEND);
      default:
        throw new IllegalArgumentException("Unsupported row kind: " + rowKind);
    }
  }
}
