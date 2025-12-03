package org.jg.strategy;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.jg.entity.Fraud;
import org.jg.entity.Transaction;
import org.jg.function.FraudDetector;
import org.jg.utils.FlinkEnv;

public class FraudTransformStrategy implements TransformStrategy<Transaction, Fraud> {

  private final FlinkEnv env;

  public FraudTransformStrategy(FlinkEnv env) {
    this.env = env;
  }

  @Override
  public DataStream<Fraud> transform(DataStream<Transaction> transactionsDs) {
    return transactionsDs
        .keyBy(Transaction::getAccountId)
        .process(new FraudDetector())
        .name("fraud-detector");
  }
}
