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
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CancellationReason;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.aws.dynamo.exceptions.ItemAlreadyExistsException;
import org.apache.iceberg.aws.dynamo.exceptions.NoSuchItemException;

public class DynamoStore {

  private final String tableName;
  private final AmazonDynamoDB client;
  private final DynamoDBTableMapper<MetadataItem, String, ?> ddb;

  public DynamoStore(String tableName, AmazonDynamoDB client, ConsistentReads readConsistency) {
    DynamoDBMapperConfig ddbConfig = DynamoDBMapperConfig.builder()
        .withTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(tableName))
        .withConsistentReads(readConsistency)
        .build();

    this.tableName = tableName;
    this.client = client;
    this.ddb = new DynamoDBMapper(client, ddbConfig).newTableMapper(MetadataItem.class);
  }

  public void save(MetadataItem item) {
    long now = Instant.now().getEpochSecond();
    if (item.getVersion() == null) {
      item.setCreatedTimestamp(now);
    }
    item.setLastUpdatedTimestamp(now);
    ddb.save(item);
  }

  public MetadataItem load(String key) {
    return ddb.load(key);
  }

  /**
   * Delete a metadata item
   *
   * @param key the table identifier
   * @return true if item was deleted, false if it didn't exist
   */
  public boolean delete(String key) {
    MetadataItem item = ddb.load(key);
    if (item == null) {
      return false;
    }

    try {
      ddb.deleteIfExists(item);
      return true;
    } catch (ConditionalCheckFailedException e) {
      return false;
    }
  }

  /**
   * Rename the table identifier of a metadata item
   *
   * @param from the source table identifier
   * @param to the target table identifier
   *
   * @throws NoSuchItemException if the source item does not exist
   * @throws ItemAlreadyExistsException if the target item already exists
   */
  public void rename(String from, String to) {
    MetadataItem fromItem = load(from);
    if (fromItem == null) {
      throw new NoSuchItemException("Item does not exist: " + from);
    }
    long now = Instant.now().getEpochSecond();

    // DynamoDBMapper transactions aren't currently compatible
    // with versioned attributes so we use the lower level API.
    Map<String, AttributeValue> toItem = new HashMap<>();
    toItem.put("tableIdentifier", new AttributeValue().withS(to));
    toItem.put("location", new AttributeValue().withS(fromItem.getLocation()));
    toItem.put("version", new AttributeValue().withN("1"));
    toItem.put("createdTimestamp", new AttributeValue().withN(String.valueOf(fromItem.getCreatedTimestamp())));
    toItem.put("lastUpdatedTimestamp", new AttributeValue().withN(String.valueOf(now)));

    Collection<TransactWriteItem> actions = Arrays.asList(
        new TransactWriteItem().withPut(
            new Put()
                .withTableName(tableName)
                .withItem(toItem)
                .withConditionExpression("attribute_not_exists(tableIdentifier)")),
        new TransactWriteItem().withDelete(
            new Delete()
                .withTableName(tableName)
                .withKey(Collections.singletonMap("tableIdentifier", new AttributeValue().withS(from)))
                .withConditionExpression("attribute_exists(tableIdentifier)"))
    );

    TransactWriteItemsRequest request = new TransactWriteItemsRequest().withTransactItems(actions);
    try {
      client.transactWriteItems(request);
    } catch (TransactionCanceledException e) {
      List<CancellationReason> reasons = e.getCancellationReasons();
      for (int i = 0; i < reasons.size(); i++) {
        final int idx = i;
        CancellationReason reason = reasons.get(idx);
        Optional.ofNullable(reason.getCode())
            .filter(code -> code.equals("ConditionalCheckFailed"))
            .ifPresent(entry -> {
              if (idx == 0) {
                throw new ItemAlreadyExistsException("Item already exists: " + to);
              } else if (idx == 1) {
                throw new NoSuchItemException("Item does not exist: " + from);
              }
            });
      }
      throw e;
    }
  }

}
