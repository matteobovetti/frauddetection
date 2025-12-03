package org.jg.pipeline;

import org.apache.flink.api.common.JobExecutionResult;
import org.jg.entity.EnrichedFraud;
import org.jg.entity.Fraud;
import org.jg.entity.Transaction;
import org.jg.strategy.*;
import org.jg.utils.FlinkEnv;

public class FraudPipeline implements FlinkPipeline {
  private final InitFlinkCatalogStrategy initFlinkCatalogStrategy;
  private final SourceStrategy<Transaction> sourceStrategy;
  private final TransformStrategy<Transaction, Fraud> transformFraudStrategy;
  private final TransformStrategy<Fraud, EnrichedFraud> transformEnrichedFraudStrategy;
  private final SinkStrategy<EnrichedFraud> sinkStrategy;

  public FraudPipeline(
      InitFlinkCatalogStrategy initStrategy,
      SourceStrategy<Transaction> sourceStrategy,
      TransformStrategy<Transaction, Fraud> transformFraudStrategy,
      TransformStrategy<Fraud, EnrichedFraud> transformEnrichedFraudStrategy,
      SinkStrategy<EnrichedFraud> sinkStrategy) {
    this.initFlinkCatalogStrategy = initStrategy;
    this.sourceStrategy = sourceStrategy;
    this.transformFraudStrategy = transformFraudStrategy;
    this.transformEnrichedFraudStrategy = transformEnrichedFraudStrategy;
    this.sinkStrategy = sinkStrategy;
  }

  @Override
  public void compose() {
    initFlinkCatalogStrategy.init();
    var transactionsDs = sourceStrategy.createSource();
    var fraudsDs = transformFraudStrategy.transform(transactionsDs);
    var enrichedFraudDs = transformEnrichedFraudStrategy.transform(fraudsDs);
    sinkStrategy.createSink(enrichedFraudDs);
  }

  @Override
  public JobExecutionResult run(FlinkEnv env) throws Exception {
    return env.getStreamEnv().execute("fraud-detection");
  }
}
