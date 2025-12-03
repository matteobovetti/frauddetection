package org.jg.config;

public class JobConfig {
  private final String bootstrapServers;
  private final String database;
  private final String transactionTable;
  private final String enrichedFraudTable;

  private JobConfig(Builder builder) {
    this.bootstrapServers = builder.bootstrapServers;
    this.database = builder.database;
    this.transactionTable = builder.transactionTable;
    this.enrichedFraudTable = builder.enrichedFraudTable;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public String getDatabase() {
    return database;
  }

  public String getTransactionTable() {
    return transactionTable;
  }

  public String getEnrichedFraudTable() {
    return enrichedFraudTable;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String bootstrapServers;
    private String database;
    private String transactionTable;
    private String enrichedFraudTable;

    public Builder bootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
      return this;
    }

    public Builder database(String database) {
      this.database = database;
      return this;
    }

    public Builder transactionTable(String transactionTable) {
      this.transactionTable = transactionTable;
      return this;
    }

    public Builder fraudTable(String fraudTable) {
      this.enrichedFraudTable = fraudTable;
      return this;
    }

    public JobConfig build() {
      return new JobConfig(this);
    }
  }
}
