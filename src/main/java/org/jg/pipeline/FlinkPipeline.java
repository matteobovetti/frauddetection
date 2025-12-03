package org.jg.pipeline;

import org.apache.flink.api.common.JobExecutionResult;
import org.jg.utils.FlinkEnv;

public interface FlinkPipeline {
  void compose();

  JobExecutionResult run(FlinkEnv env) throws Exception;
}
