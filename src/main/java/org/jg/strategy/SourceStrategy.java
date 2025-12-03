package org.jg.strategy;

import org.apache.flink.streaming.api.datastream.DataStream;

public interface SourceStrategy<T> {
  DataStream<T> createSource();
}
