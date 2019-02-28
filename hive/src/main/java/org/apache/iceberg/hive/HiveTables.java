/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.io.Closeable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseMetastoreTables;

public class HiveTables extends BaseMetastoreTables implements Closeable {
  private final HiveClientPool clients;

  public HiveTables(Configuration conf) {
    super(conf);
    this.clients = new HiveClientPool(2, conf);
  }

  @Override
  public BaseMetastoreTableOperations newTableOps(Configuration conf, String database, String table) {
    return new HiveTableOperations(conf, clients, database, table);
  }

  @Override
  public void close() {
    clients.close();
  }
}
