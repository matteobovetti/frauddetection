package org.jg.entity;

import java.io.Serializable;
import java.util.Objects;

import org.apache.flink.types.RowKind;

public final class Account implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String ID = "id";
  public static final String NAME = "name";
  public static final String UPDATED_AT = "updatedAt";

  private long id;
  private String name;
  private long updatedAt;
  private RowKind kind;

  public Account() {}

  public Account(long id, String name, long updatedAt, RowKind kind) {
    this.id = id;
    this.name = name;
    this.updatedAt = updatedAt;
    this.kind = kind;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
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
    Account that = (Account) o;
    return id == that.id && name.equals(that.name) && updatedAt == that.updatedAt;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, updatedAt);
  }

  @Override
  public String toString() {
    return "Transaction{" + " id=" + id + ", name=" + name + ", updatedAt=" + updatedAt + "}";
  }
}
