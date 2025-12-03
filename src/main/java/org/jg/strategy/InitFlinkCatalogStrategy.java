package org.jg.strategy;

import org.jg.utils.FlinkEnv;

public class InitFlinkCatalogStrategy implements InitCatalogStrategy {
  private final FlinkEnv env;

  public InitFlinkCatalogStrategy(FlinkEnv env) {
    this.env = env;
  }

  @Override
  public void init() {

    env.getTableEnv()
        .executeSql(
            "CREATE CATALOG fluss_catalog\n"
                + "WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server:9122')");

    env.getTableEnv().executeSql("USE CATALOG fluss_catalog");
    env.getTableEnv().executeSql("USE fluss");
  }
}
