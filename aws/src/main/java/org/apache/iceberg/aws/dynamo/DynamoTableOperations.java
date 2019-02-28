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

import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import java.time.Instant;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoTableOperations extends BaseMetastoreTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoTableOperations.class);
  private final String tableIdentifier;
  private final DynamoStore store;

  public DynamoTableOperations(Configuration conf, String tableIdentifier, DynamoStore store) {
    super(conf);
    this.tableIdentifier = tableIdentifier;
    this.store = store;
  }

  @Override
  public TableMetadata refresh() {
    String metadataLocation = null;
    MetadataItem item = store.load(tableIdentifier);
    if (item != null) {
      metadataLocation = item.getLocation();
    } else if (currentMetadataLocation() != null) {
      throw new NoSuchTableException("No such table: %s", tableIdentifier);
    }

    refreshFromMetadataLocation(metadataLocation);
    return current();
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    // If the metadata is already out of date, reject it
    if (base != current()) {
      throw new CommitFailedException("Stale table metadata: %s", tableIdentifier);
    } else if (base == metadata) {
      // If the metadata has not changed, return early
      LOG.info("Nothing to commit");
      return;
    }

    MetadataItem item = store.load(tableIdentifier);
    long now = Instant.now().getEpochSecond();
    if (item == null) {
      item = new MetadataItem();
      item.setTableIdentifier(tableIdentifier);
      item.setCreatedTimestamp(now);
    }

    if (!Objects.equals(currentMetadataLocation(), item.getLocation())) {
      throw new CommitFailedException(
          "metadataLocation = %s is not the same as table metadataLocation %s for %s",
          currentMetadataLocation(), item.getLocation(), tableIdentifier);
    }

    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    // Update item
    item.setLocation(newMetadataLocation);
    item.setLastUpdatedTimestamp(now);

    boolean threw = true;
    try {
      store.save(item);
      threw = false;
    } catch (ConditionalCheckFailedException e) {
      throw new CommitFailedException("Failed to commit changes: %s", tableIdentifier);
    } finally {
      if (threw) {
        // If anything went wrong, clean up the uncommitted metadata file
        io().deleteFile(newMetadataLocation);
      }
    }

    requestRefresh();
  }
}
