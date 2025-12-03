package org.jg.strategy;

import org.apache.flink.streaming.api.datastream.DataStream;

public interface SinkStrategy<T> {
  void createSink(DataStream<T> stream);
}
