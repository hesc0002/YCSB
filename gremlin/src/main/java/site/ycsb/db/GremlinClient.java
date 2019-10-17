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
        System.err.println("Could not close GremlinDB connection pool: "
            + e1.toString());
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
    try {
      // Insert query structure:
      // g.V(key).has('partition_key', key).drop()

      // Build insert query
      String deleteQuery = "";
      StringBuilder queryBuilder = new StringBuilder(deleteQuery);
      queryBuilder.append("g.V('" + key + "')");
      queryBuilder.append(".property('partition_key', '" + key + "')");
      queryBuilder.append(".drop()");
      deleteQuery = queryBuilder.toString();

      System.out.println("\nSubmitting Gremlin query to delete a vertex: " + deleteQuery);

      // Submitting remote query to the server.
      logger.info("Submitting Gremlin query to delete a vertex: {}", deleteQuery);
      ResultSet results = gremlinClient.submit(deleteQuery);
      CompletableFuture<List<Result>> completableFutureResults = results.all();
      List<Result> resultList = completableFutureResults.get();
      if ((resultList == null) || (resultList.size() == 0)) {
        logger.info("NO entry found to be deleted by query: {}", deleteQuery);
        return Status.NOT_FOUND;
      }

      CompletableFuture<Map<String, Object>> completableFutureStatusAttributes = results.statusAttributes();
      Map<String, Object> statusAttributes = completableFutureStatusAttributes.get();

      // Status code for successful query. Usually HTTP 200.
      int status = Integer.valueOf(statusAttributes.get("x-ms-status-code").toString());
      logger.info("Insert status: {}", status);

      if ((status >= 500) || (status == 408)) {
        logger.debug("Http error code: {} while deleting key: {}", status, key);
        return Status.SERVICE_UNAVAILABLE;
      } else if (status >= 400) {
        logger.debug("Http error code: {} while deleting key: {}", status, key);
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
      if (hostString == null) {
        throw new DBException(String.format(
            "Required property \"%s\" missing for GremlinClient",
            HOSTS_PROPERTY));
      }

      String[] hosts = hostString.split(",");
      String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

      String username = getProperties().getProperty(USERNAME_PROPERTY);
      String password = getProperties().getProperty(PASSWORD_PROPERTY);
      String enableSSL = getProperties().getProperty(SSL_PROPERTY, SSL_PROPERTY_DEFAULT);
      String yamlFile = getProperties().getProperty(YAML_PROPERTY, YAML_PROPERTY_DEFAULT);

      try {
        // Attempt to create the connection objects
        if (yamlFile.isEmpty()) {
          gremlinCluster = Cluster.build()
              .addContactPoints(hosts[0])
              .port(Integer.valueOf(port))
              .credentials(username, password)
              // ssl settings
              .enableSsl(Boolean.valueOf(enableSSL))
              .create();
        } else {
          gremlinCluster = Cluster.build(new File(yamlFile)).create();
        }
        gremlinClient = gremlinCluster.connect();
        logger.info("Connected to cluster: {}\nHost: {}", gremlinCluster.getPath(), gremlinClient.toString());
      } catch (FileNotFoundException e) {
        logger.error("Couldn't find the configuration file. ", e);
        e.printStackTrace();
        return;

      } catch (Exception e) {
        System.err
            .println("Could not initialize Gremlin connection pool: "
                + e.toString());
        logger.error("Could not initialize Gremlin connection pool: ", e);
        e.printStackTrace();
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
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
    try {
      // Insert query structure:
      // g.addV(key).property('id', key).property('partition_key', key)
      //  .property('field0', 'value0')....property('field9', 'value9')

      // Build insert query
      String insertQuery = "g.addV()";
      StringBuilder queryBuilder = new StringBuilder(insertQuery);
      queryBuilder.append(".property('id', '" + key + "')");
      queryBuilder.append(".property('partition_key', '" + key + "')");

      // Populate vertex properties using value map.
      for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        queryBuilder.append(".property('" + entry.getKey() + "','" + escapeGremlin(entry.getValue()) + "')");
      }
      insertQuery = queryBuilder.toString();

      //System.out.println("\nSubmitting this Gremlin insert statement: " + insertQuery);

      // Submitting remote query to the server.
      logger.info("Submitting Gremlin insert a vertex: {}", insertQuery);
      ResultSet results = gremlinClient.submit(insertQuery);

      CompletableFuture<Map<String, Object>> completableFutureStatusAttributes = results.statusAttributes();
      Map<String, Object> statusAttributes = completableFutureStatusAttributes.get();

      // Status code for successful query. Usually HTTP 200.
      int status = Integer.valueOf(statusAttributes.get("x-ms-status-code").toString());
      logger.info("Insert status: {}", status);

      if ((status >= 500) || (status == 408)) {
        logger.debug("Http error code: {} while inserting key: {}", status, key);
        return Status.SERVICE_UNAVAILABLE;
      } else if (status >= 400) {
        logger.debug("Http error code: {} while inserting key: {}", status, key);
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
    // Read query:
    // g.V(key).has('_id', key)

    // Build read query
    String readQuery = "";
    StringBuilder queryBuilder = new StringBuilder(readQuery);
    queryBuilder.append("g.V('" + key + "')");
    queryBuilder.append(".has('partition_key', '" + key + "')");

    readQuery = queryBuilder.toString();
    logger.debug("Read query: {}", readQuery);

    // Submitting remote query to the server.
    //System.out.println("\nSubmitting this Gremlin read statement: " + readQuery);
    try {
      ResultSet results = gremlinClient.submit(readQuery);
      CompletableFuture<List<Result>> completableFutureResults = results.all();
      List<Result> resultList = completableFutureResults.get();

      if (resultList == null) {
        logger.info("NO result found for query: {}", readQuery);
        return Status.NOT_FOUND;
      } else if (resultList.size() != 1) {
        logger.info("{} entries found for query: {}", resultList.size(), readQuery);
        return Status.UNEXPECTED_STATE;
      }

      // Populate fields and corresponding values in result.
      Map<Object, Object> fieldsValueMap = (Map<Object, Object>) resultList.get(0).getObject();

      if (fields != null) {
        // Fetch only requested fields/values.
        for (String field : fields) {
          result.put(field, new StringByteIterator(fieldsValueMap.get(field).toString()));
          //System.out.println("Properties :" + field + " value: " + fieldsValueMap.get(field));
        }
      } else {
        // Fetch all the fields/values.
        for (Object field : fieldsValueMap.keySet()) {
          result.put(field.toString(), new StringByteIterator(fieldsValueMap.get(field).toString()));
          //System.out.println("Properties :" + field + " value: " + fieldsValueMap.get(field));
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
    // Yet to implement the functionality.
    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
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
    try {
      // Update Query:
      // g.V(key).has('partition_key', key).property('field0', 'value0')....property('field9', 'value9')

      // Build update query
      String updateQuery = "g.V('" + key + "')";
      StringBuilder queryBuilder = new StringBuilder(updateQuery);
      queryBuilder.append(".has('partition_key', '" + key + "')");

      // Append properties and values to be updated.
      for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        queryBuilder.append(".property('" + entry.getKey() + "','" + escapeGremlin(entry.getValue()) + "')");
      }
      updateQuery = queryBuilder.toString();

      // Submitting remote query to the server.
      //System.out.println("\nSubmitting this Gremlin update query: " + updateQuery);
      ResultSet results = gremlinClient.submit(updateQuery);
      CompletableFuture<List<Result>> completableFutureResults = results.all();
      List<Result> resultList = completableFutureResults.get();
      if ((resultList == null) || (resultList.size() == 0)) {
        logger.info("NO entry found to be updated by query: {}", updateQuery);
        return Status.NOT_FOUND;
      }

      CompletableFuture<Map<String, Object>> completableFutureStatusAttributes = results.statusAttributes();
      Map<String, Object> statusAttributes = completableFutureStatusAttributes.get();

      // Status code for successful query. Usually HTTP 200.
      int status = Integer.valueOf(statusAttributes.get("x-ms-status-code").toString());
      logger.info("Gremlin Update a vertex query: {}", updateQuery);
      logger.info("Update status: {}", status);

      if ((status >= 500) || (status == 408)) {
        logger.debug("Http error code: {} while updating key: {}", status, key);
        return Status.SERVICE_UNAVAILABLE;
      } else if (status >= 400) {
        logger.debug("Http error code: {} while updating key: {}", status, key);
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
