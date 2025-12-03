package org.jg.strategy;

import org.apache.flink.streaming.api.datastream.DataStream;

public interface TransformStrategy<I, O> {
  DataStream<O> transform(DataStream<I> dataStream);
}
