package org.jg.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public final class FlinkEnv {

  private static final FlinkEnv INSTANCE = new FlinkEnv();

  private final StreamExecutionEnvironment streamEnv;
  private final StreamTableEnvironment tableEnv;

  private FlinkEnv() {
    this.streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    this.tableEnv = StreamTableEnvironment.create(streamEnv);
  }

  public static FlinkEnv getInstance() {
    return INSTANCE;
  }

  public StreamExecutionEnvironment getStreamEnv() {
    return streamEnv;
  }

  public StreamTableEnvironment getTableEnv() {
    return tableEnv;
  }
}
