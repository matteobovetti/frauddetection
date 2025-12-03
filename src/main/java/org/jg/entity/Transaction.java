package org.jg.entity;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;

import org.apache.flink.types.RowKind;

public final class Transaction implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String ID = "id";
  public static final String ACCOUNT_ID = "accountId";
  public static final String CREATED_AT = "createdAt";
  public static final String AMOUNT = "amount";

  private long id;
  private long accountId;
  private long createdAt;
  private BigDecimal amount;
  private RowKind kind;

  public Transaction() {}

  public Transaction(long id, long accountId, long createdAt, BigDecimal amount, RowKind kind) {
    this.id = id;
    this.accountId = accountId;
    this.createdAt = createdAt;
    this.amount = amount;
    this.kind = kind;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public RowKind getKind() {
    return kind;
  }

  public void setKind(RowKind kind) {
    this.kind = kind;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Transaction that = (Transaction) o;
    return id == that.id
        && accountId == that.accountId
        && createdAt == that.createdAt
        && Objects.equals(amount, that.amount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, accountId, createdAt, amount);
  }

  @Override
  public String toString() {
    return String.format(
        "Transaction{\n" + " id=%s, accountId=%s, createdAt=%s, amount=%s\n" + "}",
        id, accountId, createdAt, amount);
  }
}
