package org.jg.strategy;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import org.jg.config.JobConfig;
import org.jg.entity.Transaction;
import org.jg.serde.TransactionDeserializationSchema;
import org.jg.utils.FlinkEnv;

public class FlussSourceStrategy implements SourceStrategy<Transaction> {
  private final JobConfig config;
  private final FlinkEnv env;

  public FlussSourceStrategy(JobConfig config, FlinkEnv env) {
    this.config = config;
    this.env = env;
  }

  @Override
  public DataStream<Transaction> createSource() {
    FlussSource<Transaction> transactionSource =
        FlussSource.<Transaction>builder()
            .setBootstrapServers(config.getBootstrapServers())
            .setDatabase(config.getDatabase())
            .setTable(config.getTransactionTable())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializationSchema(new TransactionDeserializationSchema())
            .build();
    return env.getStreamEnv()
        .fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "fluss-transaction-source")
        .name("transactions-datastream");
  }
}
