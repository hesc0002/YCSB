/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * Gremlin client binding for YCSB.
 */
package site.ycsb.db;
import java.io.File;
import java.io.FileNotFoundException;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV2d0;

import org.slf4j.helpers.MessageFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import site.ycsb.*;

/**
 * Gremlin binding for YCSB framework using the apache.tinkerpop driver <a
 * href="https://mvnrepository.com/artifact/org.apache.tinkerpop/gremlin-driver">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author
 * @see <a
 * href="http://tinkerpop.apache.org/javadocs/3.3.4/core/org/apache/tinkerpop/gremlin/driver/package-summary.html">
 * Gremlin driver</a>
 */
public class GremlinClient extends DB {

  private static Logger logger = LoggerFactory.getLogger(GremlinClient.class);

  private static final String HOSTS_PROPERTY          = "gremlin.hosts";

  private static final String PORT_PROPERTY         = "gremlin.port";
  private static final String PORT_PROPERTY_DEFAULT = "443";

  private static final String USERNAME_PROPERTY     = "gremlin.username";
  private static final String PASSWORD_PROPERTY     = "gremlin.password";

  private static final String SSL_PROPERTY         = "gremlin.enableSSL";
  private static final String SSL_PROPERTY_DEFAULT = "true";

  /* Configuration file path */
  private static final String YAML_PROPERTY         = "gremlin.yaml";
  private static final String YAML_PROPERTY_DEFAULT = "";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** A singleton Cluster instance. */
  private static Cluster gremlinCluster;

  /** A singleton Gremlin instance. */
  private static Client gremlinClient;

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        gremlinClient.close();
        gremlinCluster.close();
      } catch (Exception e1) {
        System.err.println("Could not close GremlinDB connection pool: " + e1.toString());
        e1.printStackTrace();
        return;
      } finally {
        gremlinClient = null;
        gremlinCluster = null;
      }
    }
  }

  /**
   * Delete a record from the database.
   * Delete query:
   * g.V(key).has('partition_key', key).drop()
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    // Build insert query
    String deleteQuery = "";
    StringBuilder queryBuilder = new StringBuilder(deleteQuery);
    queryBuilder.append("g.V('" + key + "')");
    queryBuilder.append(".has('partition_key', '" + key + "')");
    queryBuilder.append(".drop()");
    deleteQuery = queryBuilder.toString();

    try {
      // Submitting remote query to the server.
      logger.info("Submitting Gremlin query to delete a vertex: {}", deleteQuery);
      ResultSet results = gremlinClient.submit(deleteQuery);
      CompletableFuture<List<Result>> completableFutureResults = results.all();
      List<Result> resultList = completableFutureResults.get();
      if ((resultList == null) || (resultList.size() == 0)) {
        logger.info("NO entry found to be deleted by query: {}", deleteQuery);
        return Status.NOT_FOUND;
      }

      Map<String, Object> statusAttributes = results.statusAttributes().get();

      // Status code for successful query. Usually HTTP 200.
      int status = Integer.valueOf(statusAttributes.get("x-ms-status-code").toString());
      logger.debug("Insert status: {}", status);

      if ((status >= 500) || (status == 408)) {
        logger.info("Http error code: {} while deleting key: {}", status, key);
        return Status.SERVICE_UNAVAILABLE;
      } else if (status >= 400) {
        logger.info("Http error code: {} while deleting key: {}", status, key);
        return Status.BAD_REQUEST;
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the cluster has already been initialized
      if (gremlinClient != null) {
        return;
      }

      String hostString = getProperties().getProperty(HOSTS_PROPERTY);
      String[] hosts = hostString.split(",");
      String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);
      String username = getProperties().getProperty(USERNAME_PROPERTY);
      String password = getProperties().getProperty(PASSWORD_PROPERTY);
      String enableSSL = getProperties().getProperty(SSL_PROPERTY, SSL_PROPERTY_DEFAULT);
      String yamlFile = getProperties().getProperty(YAML_PROPERTY, YAML_PROPERTY_DEFAULT);
      if (yamlFile == null) {
        throw new DBException(String.format(
            "Required property \"%s\" missing for GremlinClient",
            YAML_PROPERTY));
      }
      try {
        // Attempt to create the connection objects
        if (hostString != null) {
          final Map<String, Object> m = new HashMap<>();
          m.put("serializeResultToString", true);
          GraphSONMessageSerializerV2d0 serializer = new GraphSONMessageSerializerV2d0();
          serializer.configure(m, null);
          gremlinCluster = Cluster.build()
              .addContactPoint(hosts[0])
              .port(Integer.valueOf(port))
              .credentials(username, password)
              .enableSsl(Boolean.valueOf(enableSSL))
              .serializer(serializer)
              .create();
        } else {
          gremlinCluster = Cluster.build(new File(yamlFile))
              .create();
        }
        gremlinClient = gremlinCluster.connect();

        logger.info("Connected to cluster: {}\nHost: {}", gremlinCluster.getPath(), gremlinClient.toString());
      } catch (FileNotFoundException e) {
        System.err.println("Couldn't find the configuration file: " + yamlFile + e.toString());
        logger.error(
            MessageFormatter.format("Couldn't find the configuration file: {}", yamlFile).getMessage(),
            e);
        e.printStackTrace();
        return;

      } catch (Exception e) {
        System.err.println("Could not initialize Gremlin connection pool: " + e.toString());
        logger.error("Could not initialize Gremlin connection pool: ", e);
        e.printStackTrace();
        return;
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   * Insert query:
   * g.addV().property('id', key).property('partition_key', key)
   *  .property('field0', 'value0')....property('field9', 'value9')
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    // Build insert query
    String insertQuery = "";
    StringBuilder queryBuilder = new StringBuilder(insertQuery);
    queryBuilder.append("g.addV()");
    queryBuilder.append(".property('id', '" + key + "')");
    queryBuilder.append(".property('partition_key', '" + key + "')");

    // Populate vertex properties using value map.
    for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
      queryBuilder.append(".property('" + entry.getKey() + "','" + escapeGremlin(entry.getValue()) + "')");
    }

    insertQuery = queryBuilder.toString();

    try {
      // Submitting remote query to the server.
      logger.debug("Submitting Gremlin insert a vertex: {}", insertQuery);
      ResultSet results = gremlinClient.submit(insertQuery);
      results.all().get();

      Map<String, Object> statusAttributes = results.statusAttributes().get();

      // Status code for successful query. Usually HTTP 200.
      int status = Integer.valueOf(statusAttributes.get("x-ms-status-code").toString());
      logger.info("Insert status: {}", status);

      if ((status >= 500) || (status == 408)) {
        logger.info("Http error code: {} while inserting key: {}", status, key);
        return Status.SERVICE_UNAVAILABLE;
      } else if (status >= 400) {
        logger.info("Http error code: {} while inserting key: {}", status, key);
        return Status.BAD_REQUEST;
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   * Read query:
   * g.V(key).has('_id', key).valueMap('field0', 'field1',… 'field9')
   *  OR
   * g.V(key).has('_id', key)     // If fields == null
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    // Build read query
    String readQuery = "";
    StringBuilder queryBuilder = new StringBuilder(readQuery);
    queryBuilder.append("g.V('" + key + "')");
    queryBuilder.append(".has('partition_key', '" + key + "')");

    if (fields != null) {
      queryBuilder.append(".valueMap(");
      String prefix = "";
      for (String field : fields) {
        queryBuilder.append(prefix);
        prefix = ",";
        queryBuilder.append("'" + field + "'");
      }
      queryBuilder.append(")");
    }

    readQuery = queryBuilder.toString();
    logger.debug("Submitting read query: {}", readQuery);

    // Submitting remote query to the server.
    try {
      ResultSet results = gremlinClient.submit(readQuery);
      List<Result> resultList = results.all().get();

      if (resultList == null) {
        logger.info("NO result found for query: {}", readQuery);
        return Status.NOT_FOUND;
      }

      // Populate fields and corresponding values in result.
      Map<Object, Object> keyValueMap = (Map<Object, Object>) resultList.get(0).getObject();

      if (fields != null) {
        // Fetch only requested fields/values.
        for (String field : fields) {
          Object valueObject = ((ArrayList)keyValueMap.get(field)).get(0);
          result.put(field, new StringByteIterator(valueObject.toString()));
          logger.debug("Properties: {0} value: {1}", field, valueObject);
        }
      } else {
        // Fetch all the fields/values.
        Map<Object, Object> propertyValueMap = (Map<Object, Object>) keyValueMap.get((Object)"properties");
        for (Map.Entry<Object, Object> fieldValue : propertyValueMap.entrySet()) {
          Map<Object, Object> valueMap = (Map<Object, Object>)((ArrayList)fieldValue.getValue()).get(0);
          Object fieldObject = fieldValue.getKey();
          Object valueObject = valueMap.get((Object)"value");
          result.put(fieldValue.getKey().toString(), new StringByteIterator(valueObject.toString()));
          logger.debug("Properties: {0} value: {1}", fieldObject, valueObject);
        }
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // Not relevant.
    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * Update Query:
   * g.V(key).has('partition_key', key).property('field0', 'value0')....property('field9', 'value9')
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    // Build update query
    String updateQuery = "";
    StringBuilder queryBuilder = new StringBuilder(updateQuery);
    queryBuilder.append("g.V('" + key + "')");
    queryBuilder.append(".has('partition_key', '" + key + "')");

    // Append properties and values to be updated.
    for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
      queryBuilder.append(".property('" + entry.getKey() + "','" + escapeGremlin(entry.getValue()) + "')");
    }
    updateQuery = queryBuilder.toString();

    try {
      // Submitting remote query to the server.
      ResultSet results = gremlinClient.submit(updateQuery);
      List<Result> resultList = results.all().get();
      if ((resultList == null) || (resultList.size() == 0)) {
        logger.info("NO entry found to be updated by query: {}", updateQuery);
        return Status.NOT_FOUND;
      }

      Map<String, Object> statusAttributes = results.statusAttributes().get();

      // Status code for successful query. Usually HTTP 200.
      int status = Integer.valueOf(statusAttributes.get("x-ms-status-code").toString());
      logger.debug("Gremlin Update a vertex query: {}", updateQuery);
      logger.debug("Update status: {}", status);

      if ((status >= 500) || (status == 408)) {
        logger.info("Http error code: {} while updating key: {}", status, key);
        return Status.SERVICE_UNAVAILABLE;
      } else if (status >= 400) {
        logger.info("Http error code: {} while updating key: {}", status, key);
        return Status.BAD_REQUEST;
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Escape 2 special characters ' and \ in the input string.
   * 
   * @param str
   *          String to be modified.
   * @return String
   *          String modified by escaping special characters.
   */
  private String escapeGremlin(String str) {
    str = str.replace("\\", "\\\\");
    str = str.replace("\'", "\\'");
    return str;
  }
}
