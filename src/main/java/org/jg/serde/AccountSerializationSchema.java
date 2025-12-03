package org.jg.serde;

import org.apache.flink.types.RowKind;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.jg.entity.Account;

public class AccountSerializationSchema implements FlussSerializationSchema<Account> {

  private static final long serialVersionUID = 1L;

  @Override
  public void open(InitializationContext context) throws Exception {}

  @Override
  public RowWithOp serialize(Account value) throws Exception {
    GenericRow row = new GenericRow(3);
    row.setField(0, value.getId());
    row.setField(1, BinaryString.fromString(value.getName()));
    row.setField(2, value.getUpdatedAt());

    RowKind rowKind = value.getKind();
    switch (rowKind) {
      case INSERT:
        return new RowWithOp(row, OperationType.UPSERT);
      default:
        throw new IllegalArgumentException("Unsupported row kind: " + rowKind);
    }
  }
}
