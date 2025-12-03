package org.jg.entity;

import java.io.Serializable;
import java.util.Objects;

public final class Fraud implements Serializable {

  private static final long serialVersionUID = 1L;

  private long transactionId;
  private long accountId;

  public long getTransactionId() {
    return transactionId;
  }

  public void setTransactionId(long transactionId) {
    this.transactionId = transactionId;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long id) {
    this.accountId = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Fraud fraud = (Fraud) o;
    return accountId == fraud.accountId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountId);
  }

  @Override
  public String toString() {
    return String.format("Fraud{id=%s}", accountId);
  }
}
