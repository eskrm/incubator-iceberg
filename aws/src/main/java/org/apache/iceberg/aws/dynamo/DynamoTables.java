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

package org.apache.iceberg.aws.dynamo;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseMetastoreTables;

import static java.lang.String.format;

public class DynamoTables extends BaseMetastoreTables {
  private final Configuration conf;

  public DynamoTables(Configuration conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  protected BaseMetastoreTableOperations newTableOps(Configuration conf, String database, String table) {
    DynamoStore store = getDynamoStore();
    return new DynamoTableOperations(conf, format("%s.%s", database, table), store);
  }

  @VisibleForTesting
  DynamoStore getDynamoStore() {
    String dynamoTable = conf.get(ConfigProperties.DYNAMO_TABLE);
    Preconditions.checkArgument(dynamoTable != null, "%s cannot be empty", ConfigProperties.DYNAMO_TABLE);

    String endpoint = conf.get(ConfigProperties.DYNAMO_ENDPOINT);
    String region = conf.get(ConfigProperties.DYNAMO_REGION);

    AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
    if (endpoint != null) {
      EndpointConfiguration endpointConfiguration = new EndpointConfiguration(endpoint, region);
      builder.setEndpointConfiguration(endpointConfiguration);
    } else if (region != null) {
      builder.setRegion(region);
    }

    return new DynamoStore(dynamoTable, builder.build(), ConfigProperties.getReadConsistency(conf));
  }

  @Override
  protected String defaultWarehouseLocation(Configuration hadoopConf,
                                            String database, String table) {
    String warehouseLocation = hadoopConf.get("iceberg.metastore.warehouse.dir");
    Preconditions.checkNotNull(warehouseLocation,
        "Warehouse location is not set: iceberg.metastore.warehouse.dir=null");
    return format("%s/%s.db/%s", warehouseLocation, database, table);
  }
}
