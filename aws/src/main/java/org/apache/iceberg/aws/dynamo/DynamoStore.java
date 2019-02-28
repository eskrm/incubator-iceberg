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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.ConsistentReads;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTableMapper;

public class DynamoStore {

  private final DynamoDBTableMapper<MetadataItem, String, ?> ddb;

  public DynamoStore(String tableName, AmazonDynamoDB client, ConsistentReads readConsistency) {
    DynamoDBMapperConfig ddbConfig = DynamoDBMapperConfig.builder()
        .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(tableName))
        .withConsistentReads(readConsistency)
        .build();

    this.ddb = new DynamoDBMapper(client, ddbConfig).newTableMapper(MetadataItem.class);
  }

  public void save(MetadataItem item) {
    ddb.save(item);
  }

  public MetadataItem load(String key) {
    return ddb.load(key);
  }

}
