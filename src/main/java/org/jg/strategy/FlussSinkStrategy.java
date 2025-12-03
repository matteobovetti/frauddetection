package org.jg.strategy;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.fluss.flink.sink.FlussSink;
import org.jg.config.JobConfig;
import org.jg.entity.EnrichedFraud;
import org.jg.serde.EnrichedFraudSerializationSchema;

public class FlussSinkStrategy implements SinkStrategy<EnrichedFraud> {
  private final JobConfig config;

  public FlussSinkStrategy(JobConfig config) {
    this.config = config;
  }

  @Override
  public void createSink(DataStream<EnrichedFraud> enrichedFraudDs) {
    FlussSink<EnrichedFraud> enrichedFraudSink =
        FlussSink.<EnrichedFraud>builder()
            .setBootstrapServers(config.getBootstrapServers())
            .setDatabase(config.getDatabase())
            .setTable(config.getEnrichedFraudTable())
            .setSerializationSchema(new EnrichedFraudSerializationSchema())
            .build();
    enrichedFraudDs.sinkTo(enrichedFraudSink).name("enriched-fraud-fluss-sink");
    enrichedFraudDs.print();
  }
}
