package org.jg.serde;

import java.math.BigDecimal;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.RowKind;
import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import org.jg.entity.Transaction;

public class TransactionDeserializationSchema implements FlussDeserializationSchema<Transaction> {
  @Override
  public void open(InitializationContext context) throws Exception {}

  @Override
  public Transaction deserialize(LogRecord record) throws Exception {
    InternalRow row = record.getRow();
    long id = row.getLong(0);
    long accountId = row.getLong(1);
    long createdAt = row.getLong(2);
    BigDecimal amount = row.getDecimal(3, 10, 2).toBigDecimal();
    RowKind kind = mapChangeType(record.getChangeType());
    return new Transaction(id, accountId, createdAt, amount, kind);
  }

  @Override
  public TypeInformation<Transaction> getProducedType(RowType rowSchema) {
    return TypeInformation.of(Transaction.class);
  }

  private RowKind mapChangeType(ChangeType changeType) {
    switch (changeType) {
      case INSERT:
      case APPEND_ONLY:
        return RowKind.INSERT;
      case DELETE:
        return RowKind.DELETE;
      case UPDATE_BEFORE:
        return RowKind.UPDATE_BEFORE;
      case UPDATE_AFTER:
        return RowKind.UPDATE_AFTER;
      default:
        throw new IllegalArgumentException("Unknown ChangeType: " + changeType);
    }
  }
}
