package org.apache.iceberg.aws.dynamo;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.dynamo.exceptions.NoSuchItemException;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

import static java.lang.String.format;

public class DynamoCatalog extends BaseMetastoreCatalog {

  private final DynamoStore store;

  public DynamoCatalog(Configuration conf) {
    super(conf);
    this.store = getDynamoStore(conf);
  }

  @Override
  protected TableOperations newTableOps(
      Configuration newConf, TableIdentifier tableIdentifier) {
    return new DynamoTableOperations(newConf, tableIdentifier.toString(), store);
  }

  @Override
  protected String defaultWarehouseLocation(
      Configuration hadoopConf, TableIdentifier tableIdentifier) {
    String warehouseLocation = hadoopConf.get("iceberg.metastore.warehouse.dir");
    Preconditions.checkNotNull(warehouseLocation,
        "Warehouse location is not set: iceberg.metastore.warehouse.dir=null");
    return format("%s/%s/%s", warehouseLocation, tableIdentifier.namespace(), tableIdentifier.name());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier) {
    return store.delete(identifier.toString());
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
  DynamoStore getDynamoStore(Configuration conf) {
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
