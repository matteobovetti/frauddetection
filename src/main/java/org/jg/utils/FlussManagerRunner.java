package org.jg.utils;

import static org.jg.utils.Utils.*;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

public class FlussManagerRunner {

  public static Connection getConnection(String bootstrapServer) {
    Configuration conf = FlussManager.getConfig(bootstrapServer);
    return FlussManager.getConn(conf);
  }

  public static void createTables(Connection conn) throws Exception {
    Admin admin = FlussManager.getAdmin(conn);
    FlussManager.dropDatabase(admin, FLUSS_DB_NAME);
    FlussManager.createDatabase(admin, FLUSS_DB_NAME, "Playing with Fluss");
    FlussManager.createTable(
        admin,
        FLUSS_DB_NAME,
        TRANSACTION_LOG,
        FlussManager.createTransactionDescriptor(FlussManager.createTransactionSchema()));
    FlussManager.createTable(
        admin,
        FLUSS_DB_NAME,
        ACCOUNT_TABLE,
        FlussManager.createAccountDescriptor(FlussManager.createAccountSchema()));
    FlussManager.createTable(
        admin,
        FLUSS_DB_NAME,
        FRAUD_LOG,
        FlussManager.createFraudDescriptor(FlussManager.createFraudSchema()));
  }

  public static void writeToTables(Connection conn) throws Exception {
    final AtomicLong GLOBAL_ID = new AtomicLong(1);
    final Random RANDOM = new Random();
    TablePath transactionPath = TablePath.of(FLUSS_DB_NAME, TRANSACTION_LOG);
    TablePath accountPath = TablePath.of(FLUSS_DB_NAME, ACCOUNT_TABLE);
    FlussManager.writeToTable(TableType.TABLE, accountPath, conn, FlussManager.getAccounts());
    for (int i = 0; i < 100000; i++) {
      FlussManager.writeToTable(
          TableType.LOG, transactionPath, conn, FlussManager.getTransactions(GLOBAL_ID, RANDOM));
    }
  }

  public static void main(String[] args) throws Exception {
    initFluss(FLUSS_BOOTSTRAP_SERVER_CLIENT);
  }

  public static void initFluss(String bootstrapServer) throws Exception {
    Connection conn = getConnection(bootstrapServer);
    //createTables(conn);
    writeToTables(conn);
  }
}
