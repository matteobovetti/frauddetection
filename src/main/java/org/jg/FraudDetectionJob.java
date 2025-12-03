package org.jg;

import static org.jg.utils.Utils.*;

import org.jg.config.JobConfig;
import org.jg.pipeline.FraudPipeline;
import org.jg.strategy.*;
import org.jg.utils.FlinkEnv;

public class FraudDetectionJob {

  public static void main(String[] args) throws Exception {

    JobConfig config =
        JobConfig.builder()
            .bootstrapServers(FLUSS_BOOTSTRAP_SERVER_INTERNAL)
            .database(FLUSS_DB_NAME)
            .transactionTable(TRANSACTION_LOG)
            .fraudTable(FRAUD_LOG)
            .build();

    FlinkEnv env = FlinkEnv.getInstance();

    FraudPipeline pipeline =
        new FraudPipeline(
            new InitFlinkCatalogStrategy(env),
            new FlussSourceStrategy(config, env),
            new FraudTransformStrategy(env),
            new EnrichedFraudTransformStrategy(env),
            new FlussSinkStrategy(config));

    pipeline.compose();
    pipeline.run(env);
  }
}
