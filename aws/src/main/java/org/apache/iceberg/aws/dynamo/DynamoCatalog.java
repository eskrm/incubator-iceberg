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
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.dynamo.exceptions.NoSuchItemException;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

public class DynamoCatalog extends BaseMetastoreCatalog {

  private final Configuration conf;
  private final DynamoStore store;

  public DynamoCatalog(Configuration conf) {
    this.conf = conf;
    this.store = getDynamoStore();
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new DynamoTableOperations(conf, tableIdentifier.toString(), store);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String warehouseLocation = conf.get("dynamo.iceberg.warehouse.location");
    Preconditions.checkNotNull(warehouseLocation,
        "Warehouse location is not set: dynamo.iceberg.warehouse.location=null");
    return String.format("%s/%s/%s", warehouseLocation, tableIdentifier.namespace(), tableIdentifier.name());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    boolean deleted = store.delete(identifier.toString());
    if (deleted && purge && lastMetadata != null) {
      dropTableData(ops.io(), lastMetadata);
    }

    return deleted;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    try {
      store.rename(from.toString(), to.toString());
    } catch (NoSuchItemException e) {
      throw new NoSuchTableException(e, "Table does not exist: " + from);
    }
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



}
