package org.jg.strategy;

import static org.jg.entity.EnrichedFraud.ACCOUNT_ID;
import static org.jg.entity.EnrichedFraud.TRANSACTION_ID;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.RowKind;
import org.jg.entity.EnrichedFraud;
import org.jg.entity.Fraud;
import org.jg.utils.FlinkEnv;

public class EnrichedFraudTransformStrategy implements TransformStrategy<Fraud, EnrichedFraud> {

  private final FlinkEnv env;

  public EnrichedFraudTransformStrategy(FlinkEnv env) {
    this.env = env;
  }

  public DataStream<EnrichedFraud> transform(DataStream<Fraud> fraudsDs) {
    Table fraudsTb =
        env.getTableEnv()
            .fromDataStream(
                fraudsDs,
                Schema.newBuilder()
                    .column(TRANSACTION_ID, DataTypes.BIGINT())
                    .column(ACCOUNT_ID, DataTypes.BIGINT())
                    .columnByExpression("procTime", "PROCTIME()")
                    .build());

    env.getTableEnv().createTemporaryView("fraudsView", fraudsTb);

    Table enrichedFraudTb =
        env.getTableEnv()
            .sqlQuery(
                "SELECT\n"
                    + "  f.transactionId AS transactionId,\n"
                    + "  f.accountId AS accountId,\n"
                    + "  a.name AS name\n"
                    + "FROM fraudsView AS f\n"
                    + "LEFT JOIN account FOR SYSTEM_TIME AS OF f.procTime AS a\n"
                    + "ON f.accountId = a.id");

    return env.getTableEnv()
        .toDataStream(enrichedFraudTb)
        .map(
            row ->
                new EnrichedFraud(
                    (long) row.getField(TRANSACTION_ID),
                    (long) row.getField(EnrichedFraud.ACCOUNT_ID),
                    (String) row.getField(EnrichedFraud.NAME),
                    RowKind.INSERT));
  }
}
