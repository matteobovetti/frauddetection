package org.jg.utils;

public class Utils {

  public enum TableType {
    LOG,
    TABLE
  }

  public static final String FLUSS_DB_NAME = "fluss";

  public static final String FLUSS_BOOTSTRAP_SERVER_CLIENT = "localhost:9123";

  public static final String FLUSS_BOOTSTRAP_SERVER_INTERNAL = "coordinator-server:9122";

  public static final String TRANSACTION_LOG = "transaction";

  public static final String ACCOUNT_TABLE = "account";

  public static final String FRAUD_LOG = "fraud";
}
