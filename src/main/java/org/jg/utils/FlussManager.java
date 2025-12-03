package org.jg.utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.flink.types.RowKind;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.metadata.*;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.jg.entity.Account;
import org.jg.entity.EnrichedFraud;
import org.jg.entity.Transaction;
import org.jg.serde.AccountSerializationSchema;
import org.jg.serde.TransactionSerializationSchema;

public class FlussManager {

  public static Configuration getConfig(String boostrapServer) {
    Configuration conf = new Configuration();
    conf.setString("bootstrap.servers", boostrapServer);
    return conf;
  }

  public static Connection getConn(Configuration conf) {
    return ConnectionFactory.createConnection(conf);
  }

  public static Admin getAdmin(Connection conn) {
    return conn.getAdmin();
  }

  public static void dropDatabase(Admin admin, String db)
      throws ExecutionException, InterruptedException {
    admin.dropDatabase(db, true, true).get();
  }

  public static void createDatabase(Admin admin, String db, String comment)
      throws ExecutionException, InterruptedException {
    DatabaseDescriptor descriptor =
        DatabaseDescriptor.builder().comment(comment).customProperty("owner", "jg").build();
    admin.createDatabase(db, descriptor, true).get();
  }

  public static Schema createTransactionSchema() {
    return Schema.newBuilder()
        .column(Transaction.ID, DataTypes.BIGINT())
        .column(Transaction.ACCOUNT_ID, DataTypes.BIGINT())
        .column(Transaction.CREATED_AT, DataTypes.BIGINT())
        .column(Transaction.AMOUNT, DataTypes.DECIMAL(10, 2))
        .build();
  }

  public static TableDescriptor createTransactionDescriptor(Schema schema) {
    return TableDescriptor.builder()
        .schema(schema)
        .distributedBy(4, Transaction.ACCOUNT_ID) // few buckets for local testing
        .build();
  }

  public static Schema createAccountSchema() {
    return Schema.newBuilder()
        .column(Account.ID, DataTypes.BIGINT())
        .column(Account.NAME, DataTypes.STRING())
        .column(Account.UPDATED_AT, DataTypes.BIGINT())
        .primaryKey(Account.ID)
        .build();
  }

  public static TableDescriptor createAccountDescriptor(Schema schema) {
    return TableDescriptor.builder().schema(schema).distributedBy(4, Account.ID).build();
  }

  public static Schema createFraudSchema() {
    return Schema.newBuilder()
        .column(EnrichedFraud.TRANSACTION_ID, DataTypes.BIGINT())
        .column(EnrichedFraud.ACCOUNT_ID, DataTypes.BIGINT())
        .column(EnrichedFraud.NAME, DataTypes.STRING())
        .build();
  }

  public static TableDescriptor createFraudDescriptor(Schema schema) {
    return TableDescriptor.builder()
        .schema(schema)
        .distributedBy(4, EnrichedFraud.TRANSACTION_ID)
        .property("table.datalake.enabled", "true")
        .property("table.datalake.freshness", "30s")
        .property("table.datalake.auto-compaction", "true")
        .build();
  }

  public static void createTable(
      Admin admin, String db, String table, TableDescriptor tableDescriptor)
      throws ExecutionException, InterruptedException {
    TablePath tablePath = TablePath.of(db, table);
    admin.createTable(tablePath, tableDescriptor, false).get();
    TableInfo tableInfo = admin.getTableInfo(tablePath).get();
    System.out.println(tableInfo);
  }

  public static void writeToTable(
      Enum<Utils.TableType> tableType,
      TablePath tablePath,
      Connection connection,
      List<InternalRow> rows) {
    Table table = connection.getTable(tablePath);
    if (tableType.equals(Utils.TableType.LOG)) {
      AppendWriter writer = table.newAppend().createWriter();
      rows.forEach(writer::append);
      writer.flush();
    } else {
      UpsertWriter writer = table.newUpsert().createWriter();
      rows.forEach(writer::upsert);
      writer.flush();
    }
  }

  private static long generateRandomAccountId(Random RANDOM) {
    return 1L + (Math.abs(RANDOM.nextLong()) % 9_999_999_000L);
  }

  public static List<InternalRow> getTransactions(AtomicLong GLOBAL_ID, Random RANDOM)
      throws Exception {
    final long[] ACCOUNT_IDS = {1006L, 1007L, 1008L, 1009L};
    long fraudAccountId = ACCOUNT_IDS[RANDOM.nextInt(ACCOUNT_IDS.length)];
    List<Transaction> transactions = new ArrayList<>();
    transactions.add(
        new Transaction(
            GLOBAL_ID.getAndIncrement(),
            fraudAccountId,
            System.currentTimeMillis(),
            new BigDecimal("0.8"),
            RowKind.INSERT));
    transactions.add(
        new Transaction(
            GLOBAL_ID.getAndIncrement(),
            fraudAccountId,
            System.currentTimeMillis(),
            new BigDecimal("1001.00"),
            RowKind.INSERT));
    for (int i = 0; i < 8; i++) {
      transactions.add(
          new Transaction(
              GLOBAL_ID.getAndIncrement(),
              generateRandomAccountId(RANDOM),
              System.currentTimeMillis(),
              BigDecimal.valueOf(10 + RANDOM.nextInt(500)),
              RowKind.INSERT));
    }
    TransactionSerializationSchema schema = new TransactionSerializationSchema();
    schema.open(null);
    return transactions.stream()
        .map(
            tx -> {
              try {
                RowWithOp rowWithOp = schema.serialize(tx);
                return rowWithOp.getRow();
              } catch (Exception e) {
                throw new RuntimeException("Serialization failed for transaction: " + tx, e);
              }
            })
        .collect(Collectors.toList());
  }

  public static List<InternalRow> getAccounts() throws Exception {
    List<Account> accounts =
        List.of(
            new Account(1006L, "account1006", System.currentTimeMillis(), RowKind.INSERT),
            new Account(1007L, "account1007", System.currentTimeMillis(), RowKind.INSERT),
            new Account(1008L, "account1008", System.currentTimeMillis(), RowKind.INSERT),
            new Account(1009L, "account1009", System.currentTimeMillis(), RowKind.INSERT));
    AccountSerializationSchema schema = new AccountSerializationSchema();
    schema.open(null);
    return accounts.stream()
        .map(
            account -> {
              try {
                RowWithOp rowWithOp = schema.serialize(account);
                return rowWithOp.getRow();
              } catch (Exception e) {
                throw new RuntimeException("Serialization failed for account: " + account, e);
              }
            })
        .collect(Collectors.toList());
  }
}
