/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.mysql;

import com.google.common.base.Strings;
import io.cdap.plugin.db.batch.TransactionIsolationLevel;
import io.cdap.plugin.db.connector.AbstractDBSpecificConnectorConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.cdap.plugin.mysql.MysqlConstants.TRANSACTION_ISOLATION_LEVEL;

/**
 * Configuration for Mysql Connector
 */
public class MysqlConnectorConfig extends AbstractDBSpecificConnectorConfig {

  private static final String MYSQL_CONNECTION_STRING_FORMAT = "jdbc:mysql://%s:%s";
  private static final String JDBC_PROPERTY_CONNECT_TIMEOUT = "connectTimeout";
  private static final String JDBC_PROPERTY_SOCKET_TIMEOUT = "socketTimeout";
  private static final String JDBC_REWRITE_BATCHED_STATEMENTS = "rewriteBatchedStatements";

//  @Name(MysqlConstants.TRANSACTION_ISOLATION_LEVEL)
//  @Description("The transaction isolation level for the database session.")
//  @Macro
//  @Nullable
//  private String transactionIsolationLevel;

  public MysqlConnectorConfig(String host, int port, String user, String password, String jdbcPluginName,
                              String connectionArguments) {

    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.jdbcPluginName = jdbcPluginName;
    this.connectionArguments = connectionArguments;
  }

  @Override
  public String getConnectionString() {
    return String.format(MYSQL_CONNECTION_STRING_FORMAT, host, getPort());
  }

  @Override
  public int getDefaultPort() {
    return 3306;
  }

  @Override
  public Properties getConnectionArgumentsProperties() {
    Properties prop = super.getConnectionArgumentsProperties();
    // the unit below is milli-second
    prop.put(JDBC_PROPERTY_CONNECT_TIMEOUT, "20000");
    prop.put(JDBC_PROPERTY_SOCKET_TIMEOUT, "20000");
    prop.put(JDBC_REWRITE_BATCHED_STATEMENTS, "true");
    return prop;
  }

  public String getTransactionIsolationLevel() {

//    if (StringUtils.isNotBlank(transactionIsolationLevel)) {
//      return transactionIsolationLevel;
//    }

    if (getConnectionArgumentsProperties() == null ||
            Strings.isNullOrEmpty((String) getConnectionArgumentsProperties().get(TRANSACTION_ISOLATION_LEVEL))) {
      return TransactionIsolationLevel.Level.TRANSACTION_SERIALIZABLE.name();
    }

    return TransactionIsolationLevel.Level.valueOf(
            getConnectionArgumentsProperties().get(TRANSACTION_ISOLATION_LEVEL).toString()).name();
  }

  @Override
  public Map<String, String> getAdditionalArguments() {
    Map<String, String> additonalArguments = new HashMap<>();
    if (getTransactionIsolationLevel() != null) {
      additonalArguments.put(TransactionIsolationLevel.CONF_KEY, getTransactionIsolationLevel());
    }
    return additonalArguments;
  }
}
