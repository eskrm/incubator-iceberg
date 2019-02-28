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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.ConsistentReads;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.lang.String.format;
import static org.apache.iceberg.PartitionSpec.builderFor;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamoTablesTest {

  private static final String DATABASE_NAME = "mydb";
  private static final String TABLE_NAME =  "mytbl";
  private static final String TABLE_IDENTIFIER = format("%s.%s", DATABASE_NAME, TABLE_NAME);
  private static final Schema SCHEMA =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
  private static final PartitionSpec PARTITION_SPEC = builderFor(SCHEMA).identity("id").build();
  private static DynamoDBProxyServer server;

  private Configuration conf;
  private AmazonDynamoDB client;
  private DynamoStore store;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void startServer() throws Exception {
    // Add native libraries to path
    Path nativeLibDir = Paths.get("build/libs");
    if (Files.notExists(nativeLibDir)) {
      Files.createDirectory(nativeLibDir);
      Arrays.stream(System.getProperty("java.class.path").split(":"))
          .filter(path -> path.matches(".*\\.(dll|dylib|so)$"))
          .map(Paths::get)
          .forEach(lib -> {
            try {
              Files.copy(lib, nativeLibDir.resolve(lib.getFileName()));
            } catch (IOException e) {
              throw new RuntimeException("Failed to copy native dependency", e);
            }
          });
    }

    System.setProperty("sqlite4java.library.path", nativeLibDir.toString());
    String[] args = { "-inMemory", "-port", "8000" };
    server = ServerRunner.createServerFromCommandLineArgs(args);
    server.start();
  }

  @AfterClass
  public static void stopServer() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  @Before
  public void setup() throws Exception {
    conf = new Configuration();

    File tempDir = temp.newFolder();
    conf.set("iceberg.metastore.warehouse.dir", tempDir.getAbsolutePath());

    String dynamoTable = conf.get(ConfigProperties.DYNAMO_TABLE);
    String endpoint = conf.get(ConfigProperties.DYNAMO_ENDPOINT);
    String region = conf.get(ConfigProperties.DYNAMO_REGION);
    EndpointConfiguration endpointConfig = new EndpointConfiguration(endpoint, region);
    client = AmazonDynamoDBClientBuilder.standard()
        .withEndpointConfiguration(endpointConfig)
        .build();

    CreateTableRequest tableRequest = new CreateTableRequest()
        .withTableName(dynamoTable)
        .withKeySchema(new KeySchemaElement("tableIdentifier", KeyType.HASH))
        .withAttributeDefinitions(new AttributeDefinition("tableIdentifier", ScalarAttributeType.S))
        .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
    client.createTable(tableRequest);
    store = new DynamoStore(dynamoTable, client, ConsistentReads.EVENTUAL);
  }

  @After
  public void cleanup() {
    client.deleteTable(conf.get(ConfigProperties.DYNAMO_TABLE));
  }

  private List<String> metadataFiles(String basePath, String extension) {
    Path tablePath = Paths.get(basePath, DATABASE_NAME + ".db", TABLE_NAME, "metadata");
    return Arrays.stream(tablePath.toFile().listFiles())
        .map(File::getAbsolutePath)
        .filter(f -> f.endsWith(extension))
        .collect(Collectors.toList());
  }

  @Test
  public void testCreate() {
    new DynamoTables(conf).create(SCHEMA, PARTITION_SPEC, DATABASE_NAME, TABLE_NAME);
    MetadataItem item = store.load(TABLE_IDENTIFIER);
    Assert.assertNotNull(item);
    Assert.assertEquals(TABLE_IDENTIFIER, item.getTableIdentifier());
    Assert.assertEquals(1, item.getVersion().longValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConfigFailure() {
    Configuration invalidConf = new Configuration(conf);
    invalidConf.unset(ConfigProperties.DYNAMO_TABLE);
    new DynamoTables(invalidConf).create(SCHEMA, PARTITION_SPEC, DATABASE_NAME, TABLE_NAME);
  }

  @Test
  public void testUpdate() {
    Table table = new DynamoTables(conf).create(SCHEMA, PARTITION_SPEC, DATABASE_NAME, TABLE_NAME);
    table.refresh();
    table.updateSchema().addColumn("data", Types.LongType.get()).commit();

    Schema altered = new Schema(Types.StructType.of(
        required(1, "id", Types.LongType.get()),
        optional(2, "data", Types.LongType.get())).fields());

    // Only 2 snapshots should exist and no manifests should exist
    String basePath = conf.get("iceberg.metastore.warehouse.dir");
    Assert.assertEquals(2, metadataFiles(basePath, getFileExtension(conf)).size());
    Assert.assertEquals(0, metadataFiles(basePath, ".avro").size());
    Assert.assertEquals(altered.asStruct(), table.schema().asStruct());
  }

  @Test(expected = CommitFailedException.class)
  public void testUpdateFailure() {
    new DynamoTables(conf).create(SCHEMA, PARTITION_SPEC, DATABASE_NAME, TABLE_NAME);
    MetadataItem item = store.load(TABLE_IDENTIFIER);

    Tables tables = new DynamoTables(conf) {
      @Override
      DynamoStore getDynamoStore() {
        // Simulate competing writes
        DynamoStore store = mock(DynamoStore.class);
        when(store.load(TABLE_IDENTIFIER)).thenReturn(item);
        doThrow(new ConditionalCheckFailedException("Failed to save"))
            .when(store).save(any());
        return store;
      }
    };

    tables.load(TABLE_IDENTIFIER)
        .updateSchema()
        .addColumn("data", Types.LongType.get())
        .commit();
  }

  @Test
  public void testConcurrentFastAppends() {
    Tables tables = new DynamoTables(conf);
    tables.create(SCHEMA, PARTITION_SPEC, TABLE_IDENTIFIER);
    Table table = tables.load(TABLE_IDENTIFIER);
    Table anotherTable = tables.load(TABLE_IDENTIFIER);

    String fileName = UUID.randomUUID().toString();
    DataFile file = DataFiles.builder(table.spec())
            .withPath(FileFormat.PARQUET.addExtension(fileName))
            .withRecordCount(2)
            .withFileSizeInBytes(0)
            .build();

    ExecutorService executorService = MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    Tasks.foreach(table, anotherTable)
            .stopOnFailure().throwFailureWhenFinished()
            .executeWith(executorService)
            .run(tbl -> {
              for (int numCommittedFiles = 0; numCommittedFiles < 10; numCommittedFiles++) {
                long commitStartTime = System.currentTimeMillis();
                tbl.newFastAppend().appendFile(file).commit();
                long commitEndTime = System.currentTimeMillis();
                long commitDuration = commitEndTime - commitStartTime;
                try {
                  TimeUnit.MILLISECONDS.sleep(200 - commitDuration);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            });

    table.refresh();
    Assert.assertEquals(20, table.currentSnapshot().manifests().size());
  }

}
