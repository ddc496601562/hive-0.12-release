/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Container for metadata read from the metadata server.
 * Prior to release 0.5, InputJobInfo was a key part of the public API, exposed directly
 * to end-users as an argument to
 * {@link HCatInputFormat#setInput(org.apache.hadoop.mapreduce.Job, InputJobInfo)}.
 * Going forward, we plan on treating InputJobInfo as an implementation detail and no longer
 * expose to end-users. Should you have a need to use InputJobInfo outside HCatalog itself,
 * please contact the developer mailing list before depending on this class.
 * 利用：
 * 在提交job时候会连接相应的数据库，初始化好这个对象，然后使用Java序列化这个对象并存放在conf中  
 * 初始设置的值有：databaseName、tableName、filter ，然后通过
 * InitializeInput.getInputJobInfo(Configuration conf, InputJobInfo inputJobInfo, String locationFilter)链接元数据库
 * 方法初始化tableInfo、partitions这几个属性值  
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class InputJobInfo implements Serializable {
  /** The serialization version */
  private static final long serialVersionUID = 1L;
  /** The db and table names. */
  private final String databaseName;
  private final String tableName;
  /** meta information of the table to be read from ,主要是col list 和patition list 以及元数据库中读出的Table对象 */
  private HCatTableInfo tableInfo;
  /** The partition filter , such as :pdate='1991-02-14' ，分partition读取*/
  private String filter;
  /** The list of partitions matching the filter,
   * 包含一个partition的 storageHandlerClassName、inputFormatClassName、outputFormatClassName、serdeClassName、location、schema
   * 对应hive中一个partition的读取信息SDS表中的信息
   * 这里的transient是无用的，因为在实现的时候使用的是ArrayList，而ArrayList实现的Serializable的writeObject方法，会将这个List序列化     why  ？？
   */
  transient private List<PartInfo> partitions;
  /** implementation specific job properties */
  private Properties properties;
  /**
   * Initializes a new InputJobInfo
   * for reading data from a table.
   * @param databaseName the db name
   * @param tableName the table name
   * @param filter the partition filter
   * @param properties implementation specific job properties
   */
  public static InputJobInfo create(String databaseName,
                    String tableName,
                    String filter,
                    Properties properties) {
    return new InputJobInfo(databaseName, tableName, filter, properties);
  }

  /**
   * Initializes a new InputJobInfo
   * for reading data from a table.
   * @param databaseName the db name
   * @param tableName the table name
   * @param filter the partition filter
   */
  @Deprecated
  public static InputJobInfo create(String databaseName,
                    String tableName,
                    String filter) {
    return create(databaseName, tableName, filter, null);
  }


  private InputJobInfo(String databaseName,
             String tableName,
             String filter,
             Properties properties) {
    this.databaseName = (databaseName == null) ?
      MetaStoreUtils.DEFAULT_DATABASE_NAME : databaseName;
    this.tableName = tableName;
    this.filter = filter;
    this.properties = properties == null ? new Properties() : properties;
  }

  /**
   * Gets the value of databaseName
   * @return the databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Gets the value of tableName
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Gets the table's meta information
   * @return the HCatTableInfo
   */
  public HCatTableInfo getTableInfo() {
    return tableInfo;
  }

  /**
   * set the tablInfo instance
   * this should be the same instance
   * determined by this object's DatabaseName and TableName
   * @param tableInfo
   */
  void setTableInfo(HCatTableInfo tableInfo) {
    this.tableInfo = tableInfo;
  }

  /**
   * Gets the value of partition filter
   * @return the filter string
   */
  public String getFilter() {
    return filter;
  }

  /**
   * @return partition info
   */
  public List<PartInfo> getPartitions() {
    return partitions;
  }

  /**
   * @return partition info  list
   */
  void setPartitions(List<PartInfo> partitions) {
    this.partitions = partitions;
  }

  /**
   * Set/Get Property information to be passed down to *StorageHandler implementation
   * put implementation specific storage handler configurations here
   * @return the implementation specific job properties
   */
  public Properties getProperties() {
    return properties;
  }

  /**
   * Serialize this object, compressing the partitions which can exceed the
   * allowed jobConf size.
   * @see <a href="https://issues.apache.org/jira/browse/HCATALOG-453">HCATALOG-453</a>
   */
  private void writeObject(ObjectOutputStream oos)
    throws IOException {
    oos.defaultWriteObject();
    Deflater def = new Deflater(Deflater.BEST_COMPRESSION);
    ObjectOutputStream partInfoWriter =
      new ObjectOutputStream(new DeflaterOutputStream(oos, def));
    partInfoWriter.writeObject(partitions);
    partInfoWriter.close();
  }

  /**
   * Deserialize this object, decompressing the partitions which can exceed the
   * allowed jobConf size.
   * @see <a href="https://issues.apache.org/jira/browse/HCATALOG-453">HCATALOG-453</a>
   */
  @SuppressWarnings("unchecked")
  private void readObject(ObjectInputStream ois)
    throws IOException, ClassNotFoundException {
    ois.defaultReadObject();
    ObjectInputStream partInfoReader =
      new ObjectInputStream(new InflaterInputStream(ois));
    partitions = (List<PartInfo>)partInfoReader.readObject();
  }
}
