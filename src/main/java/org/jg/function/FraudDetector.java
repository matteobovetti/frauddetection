package org.jg.function;

import java.math.BigDecimal;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.jg.entity.Fraud;
import org.jg.entity.Transaction;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Fraud> {

  private static final long serialVersionUID = 1L;

  private static final BigDecimal SMALL_AMOUNT = BigDecimal.valueOf(1.00);
  private static final BigDecimal LARGE_AMOUNT = BigDecimal.valueOf(1000.00);
  private static final long ONE_MINUTE = 60 * 1000;

  private transient ValueState<Boolean> flagState;
  private transient ValueState<Long> timerState;

  @Override
  public void open(OpenContext openContext) {
    ValueStateDescriptor<Boolean> flagDescriptor =
        new ValueStateDescriptor<>("flag", Types.BOOLEAN);
    flagState = getRuntimeContext().getState(flagDescriptor);
    ValueStateDescriptor<Long> timerDescriptor =
        new ValueStateDescriptor<>("timer-state", Types.LONG);
    timerState = getRuntimeContext().getState(timerDescriptor);
  }

  @Override
  public void processElement(Transaction transaction, Context context, Collector<Fraud> collector)
      throws Exception {
    Boolean lastTransactionWasSmall = flagState.value();
    if (lastTransactionWasSmall != null) {
      if (transaction.getAmount().compareTo(LARGE_AMOUNT) > 0) {
        Fraud fraud = new Fraud();
        fraud.setTransactionId(transaction.getId());
        fraud.setAccountId(transaction.getAccountId());
        collector.collect(fraud);
      }
      cleanUp(context);
    }
    if (transaction.getAmount().compareTo(SMALL_AMOUNT) < 0) {
      flagState.update(true);
      long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
      context.timerService().registerProcessingTimeTimer(timer);
      timerState.update(timer);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Fraud> out) {
    timerState.clear();
    flagState.clear();
  }

  private void cleanUp(Context ctx) throws Exception {
    Long timer = timerState.value();
    ctx.timerService().deleteProcessingTimeTimer(timer);
    timerState.clear();
    flagState.clear();
  }
}
