/**
 * Copyright (c) 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted(tm), formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.xdata.ops.io
import org.scalatest.{BeforeAndAfterAll, Tag}
import java.io.ByteArrayInputStream
import org.apache.hadoop.hbase.client._;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import org.apache.spark.rdd.RDD

import software.uncharted.xdata.spark.SparkFunSpec

object HBaseConnectorTest extends Tag("hbc.test")
class HBaseConnectorTest extends SparkFunSpec with BeforeAndAfterAll {

  protected override def beforeAll() = {
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "uscc0-node08.uncharted.software")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.master", "hdfs://uscc0-master0.uncharted.software:60000")
    config.set("hbase.client.keyvalue.maxsize", "0")
    val connection = ConnectionFactory.createConnection(config)
    val admin = connection.getAdmin
    List(testTable).foreach { tableName =>
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      tableDescriptor.addFamily(new HColumnDescriptor(testColFamilyName))
      admin.createTable(tableDescriptor)
    }
    connection.close()
  }

  protected override def afterAll() = {
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "uscc0-node08.uncharted.software")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.master", "hdfs://uscc0-master0.uncharted.software:60000")
    config.set("hbase.client.keyvalue.maxsize", "0")
    val connection = ConnectionFactory.createConnection(config)
    val admin = connection.getAdmin

    admin.disableTable(TableName.valueOf(testTable))
    admin.deleteTable(TableName.valueOf(testTable))
    admin.disableTable(TableName.valueOf(nonExistantTable))
    admin.deleteTable(TableName.valueOf(nonExistantTable))
    admin.disableTable(TableName.valueOf(nonExistantTable2))
    admin.deleteTable(TableName.valueOf(nonExistantTable2))
    connection.close()

    hbc.close
  }


  private val configFile = Seq("/home/asuri/Documents/hbase-site.xml")
  private val incorrectConfigFile = Seq("/home/asuri/Documents/hbase-site-incorrect.xml")
  private lazy val hbc = HBaseConnector(configFile)


  private val testColFamilyName = "tileData"
  private val nonExistantColFamilyName = "nonExistantCol"

  private val testTable = "testTable"
  private val nonExistantTable = "nonExistantTable"
  private val nonExistantTable2 = "nonExistantTable2"

  private val qualifier1 = "layer"
  private val qualifier2 = "anotherLayer"

  private val data = Seq[Byte](0, 1, 2, 3, 4, 5)

  private val data2 = Array(("3", data.toSeq), ("4", data.toSeq))
  private val data3 = Array(("5", data.toSeq), ("6", data.toSeq))
  private val data4 = Array(("7", data.toSeq), ("8", data.toSeq))
  describe("HBaseConnectorTest") {

    describe("getConnection") {
      it("should create a connection object based on the config file that allows client to write to HBase") {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data2)
        val testHBCObject = HBaseConnector(configFile)
        assertResult(true)(testHBCObject.writeRows(tableName =  testTable, colFamilyName = testColFamilyName)(rddData))
        testHBCObject.close
      }

      // //is it possible that the config file can be incorrect?
      // it("should return false when connection is not established") {
      //   val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data2)
      //   val testHBCObject = HBaseConnector(incorrectConfigFile)
      //   assertResult(false)(testHBCObject.writeRows(tableName = testTable, colFamilyName = testColFamilyName)(rddData))
      //   testHBCObject.close
      // }
    }

    describe("writeRows") {
      it("should return true when rows are inserted into database") {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data2)
        val result = hbc.writeRows(tableName = testTable, colFamilyName = testColFamilyName)(rddData)
        assertResult(true)(result)
      }

      // it("should return false and not create a table when admin object isn't created") {
      //   val testHBCObject = HBConn(incorrectConfigFile)
      //   assertResult(false)(testHBCObject.writeRows(tableName = nonExistantTable, colFamilyName = testColFamilyName)(data))
      //   testHBCObject.close
      // }

      it("should create table and write into table when non existant table is given") {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data2)
        assertResult(true)(hbc.writeRows(tableName = nonExistantTable2, colFamilyName = testColFamilyName)(rddData))
      }

      it("should add the rows to the specified table in HBase") {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data4)
        hbc.writeRows(tableName = testTable, colFamilyName = testColFamilyName)(rddData)

        //create connection
        val config = HBaseConfiguration.create()
        config.set("hbase.zookeeper.quorum", "uscc0-node08.uncharted.software")
        config.set("hbase.zookeeper.property.clientPort", "2181")
        config.set("hbase.master", "hdfs://uscc0-master0.uncharted.software:60000")
        config.set("hbase.client.keyvalue.maxsize", "0")
        val connection = ConnectionFactory.createConnection(config)
        //get table
        val rowDataTable = connection.getTable(TableName.valueOf(testTable))
        val rowData = rowDataTable.get(new Get("7".getBytes).addFamily(testColFamilyName.getBytes)).value().toSeq
        //get value for specific rowID
        assertResult(data)(rowData)

        connection.close
      }

      it("should return false when incorrect family name is passed in") {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data4)
        assertResult(false)(hbc.writeRows(tableName = testTable, colFamilyName = nonExistantColFamilyName)(rddData))
      }

      it("should write data into column qualifier when specified") {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data4)
        val result = hbc.writeRows(tableName = testTable, colFamilyName = testColFamilyName, qualifierName = qualifier1)(rddData)
        assertResult(true)(result)
      }

      it("should store the data in a column qualifier when specified") {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data3)
        hbc.writeRows(tableName = testTable, colFamilyName = testColFamilyName, qualifierName = qualifier2)(rddData)
        //create connection
        val config = HBaseConfiguration.create()
        config.set("hbase.zookeeper.quorum", "uscc0-node08.uncharted.software")
        config.set("hbase.zookeeper.property.clientPort", "2181")
        config.set("hbase.master", "hdfs://uscc0-master0.uncharted.software:60000")
        config.set("hbase.client.keyvalue.maxsize", "0")
        val connection = ConnectionFactory.createConnection(config)
        //get table
        val rowDataTable = connection.getTable(TableName.valueOf(testTable))
        val rowData = rowDataTable.get(new Get("5".getBytes).addColumn(testColFamilyName.getBytes, qualifier2.getBytes)).value().toSeq
        assertResult(data)(rowData)
      }

    }

    //private method. Check if it works by using public methods.
    describe("tableExistsOrCreate") {
      it("should return true if table exists.") {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data2)
        val result = hbc.writeRows(tableName = testTable, colFamilyName = testColFamilyName)(rddData)
        assertResult(true)(result)
      }

      it("should create a table in HBase when specified table does not exist") {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data2)
        //create connection
        val config = HBaseConfiguration.create()
        config.set("hbase.zookeeper.quorum", "uscc0-node08.uncharted.software")
        config.set("hbase.zookeeper.property.clientPort", "2181")
        config.set("hbase.master", "hdfs://uscc0-master0.uncharted.software:60000")
        config.set("hbase.client.keyvalue.maxsize", "0")
        val connection = ConnectionFactory.createConnection(config)
        val isTableExist = connection.getAdmin().tableExists(TableName.valueOf(nonExistantTable))
        //check if table exists
        assertResult(false)(isTableExist)

        val result = hbc.writeRows(tableName = nonExistantTable, colFamilyName = testColFamilyName)(rddData)

        //use connection to see if table exists Now
        val isTableExistNow = connection.getAdmin().tableExists(TableName.valueOf(nonExistantTable))
        assertResult(true)(isTableExistNow)

        connection.close
      }
    }
  }
}
