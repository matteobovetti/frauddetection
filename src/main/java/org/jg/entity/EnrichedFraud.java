package org.jg.entity;

import java.io.Serializable;

import org.apache.flink.types.RowKind;

public final class EnrichedFraud implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String TRANSACTION_ID = "transactionId";
  public static final String ACCOUNT_ID = "accountId";
  public static final String NAME = "name";

  private long transactionId;
  private long accountId;
  private String name;
  private RowKind kind;

  public EnrichedFraud() {}

  public EnrichedFraud(long transactionId, long accountId, String name, RowKind kind) {
    this.transactionId = transactionId;
    this.accountId = accountId;
    this.name = name;
    this.kind = kind;
  }

  public long getTransactionId() {
    return transactionId;
  }

  public void setTransactionId(long transactionId) {
    this.transactionId = accountId;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public RowKind getKind() {
    return kind;
  }

  public void setKind(RowKind kind) {
    this.kind = kind;
  }

  @Override
  public String toString() {
    return String.format(
        "EnrichedFraud{\n" + " transactionId=%s, accountId=%s, name=%s\n" + "}",
        transactionId, accountId, name);
  }
}
