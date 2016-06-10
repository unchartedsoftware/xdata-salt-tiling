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
import org.apache.hadoop.hbase.client._ //scalastyle:ignore
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName

import org.apache.spark.rdd.RDD

import software.uncharted.xdata.spark.SparkFunSpec

object HBaseTest extends Tag("hbc.test")

class HBaseConnectorTest extends SparkFunSpec with BeforeAndAfterAll {

  private def createConfig() = {
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "uscc0-node08.uncharted.software")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.master", "hdfs://uscc0-master0.uncharted.software:60000")
    config.set("hbase.client.keyvalue.maxsize", "0")
    ConnectionFactory.createConnection(config)
  }

  protected override def beforeAll() = {
    val connection = createConfig()
    val admin = connection.getAdmin

    val tableName = TableName.valueOf(testTable)
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

    val tableDescriptor = new HTableDescriptor(tableName)
    tableDescriptor.addFamily(new HColumnDescriptor(testColFamilyName))
    tableDescriptor.addFamily(new HColumnDescriptor(testColMetaDataName))
    admin.createTable(tableDescriptor)

    connection.close()
  }

  protected override def afterAll() = {
    val connection = createConfig()
    val admin = connection.getAdmin

    admin.disableTable(TableName.valueOf(testTable))
    admin.deleteTable(TableName.valueOf(testTable))
    admin.disableTable(TableName.valueOf(nonExistentTable))
    admin.deleteTable(TableName.valueOf(nonExistentTable))
    admin.disableTable(TableName.valueOf(nonExistentTable2))
    admin.deleteTable(TableName.valueOf(nonExistentTable2))
    admin.disableTable(TableName.valueOf(nonExistentTable3))
    admin.deleteTable(TableName.valueOf(nonExistentTable3))
    admin.disableTable(TableName.valueOf(nonExistentTable4))
    admin.deleteTable(TableName.valueOf(nonExistentTable4))
    connection.close()

    hbc.close
  }


  private val configFile = Seq(classOf[HBaseConnectorTest].getResource("/hbase-site.xml").toURI.getPath)
  private lazy val hbc = HBaseConnector(configFile)


  private val testColFamilyName = "tileData"
  private val testColMetaDataName = "tileMetaData"

  private val testTable = "testTable"
  private val nonExistentTable = "nonExistentTable"
  private val nonExistentTable2 = "nonExistentTable2"
  private val nonExistentTable3 = "nonExistentTable3"
  private val nonExistentTable4 = "nonExistentTable4"

  private val qualifier1 = "layer"
  private val qualifier2 = "anotherLayer"

  private val data = Seq[Byte](0, 1, 2, 3, 4, 5)
  //remove toSeq and run again
  private val data2 = Array(("3,4,5", data), ("4,4,5", data))
  private val data3 = Array(("5,4,5", data), ("6,4,5", data))
  private val data4 = Array(("7,4,5", data), ("8,4,5", data))
  describe("HBaseConnectorTest") {

    describe("getConnection") {
      it("should create a connection object based on the config file that allows client to write to HBase", HBaseTest) {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data2)
        val testHBCObject = HBaseConnector(configFile)
        assertResult(true)(testHBCObject.writeTileData(tableName =  testTable)(rddData))
        testHBCObject.close
      }
    }

    describe("writeTileData") {
      it("should return true when rows are inserted into database", HBaseTest) {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data2)
        val result = hbc.writeTileData(tableName = testTable)(rddData)
        assertResult(true)(result)
      }

      it("should add the rows to the specified table in HBase", HBaseTest) {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data4)
        hbc.writeTileData(tableName = testTable)(rddData)

        //create connection
        val connection = createConfig()
        //get table
        val rowDataTable = connection.getTable(TableName.valueOf(testTable))
        val rowData = rowDataTable.get(new Get("7,4,5".getBytes).addFamily(testColFamilyName.getBytes)).value().toSeq
        //get value for specific rowID
        assertResult(data)(rowData)

        connection.close()
      }

      it("should write data into column qualifier when specified", HBaseTest) {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data4)
        val result = hbc.writeTileData(tableName = testTable, qualifierName = qualifier1)(rddData)
        assertResult(true)(result)
      }

      it("should store the data in a column qualifier when specified", HBaseTest) {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data3)
        hbc.writeTileData(tableName = testTable, qualifierName = qualifier2)(rddData)
        //create connection
        val connection = createConfig()
        //get table
        val rowDataTable = connection.getTable(TableName.valueOf(testTable))
        val rowData = rowDataTable.get(new Get("5,4,5".getBytes).addColumn(testColFamilyName.getBytes, qualifier2.getBytes)).value().toSeq
        assertResult(data)(rowData)
        connection.close()
      }

      it("should create table and write into table when non-existent table is given", HBaseTest) {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data2)
        assertResult(true)(hbc.writeTileData(tableName = nonExistentTable2)(rddData))
      }

      it("should create a table in HBase when specified table does not exist", HBaseTest) {
        val rddData: RDD[(String, Seq[Byte])] = sc.parallelize(data2)
        val connection = createConfig()
        val isTableExist = connection.getAdmin.tableExists(TableName.valueOf(nonExistentTable))
        //check if table exists
        assertResult(false)(isTableExist)

        hbc.writeTileData(tableName = nonExistentTable)(rddData)

        //use connection to see if table exists Now
        val isTableExistNow = connection.getAdmin.tableExists(TableName.valueOf(nonExistentTable))
        assertResult(true)(isTableExistNow)

        connection.close()
      }

      describe("#writeMetaData") {

        it("should return true when data is written into the row", HBaseTest) {
          assertResult(true)(hbc.writeMetaData(tableName =  testTable, rowID = "testRowID", data = data))
        }

        it("should add the rows to the specified table in HBase", HBaseTest) {
          hbc.writeMetaData(tableName = testTable, rowID = "testRowID2", data = data)
          //create connection
          val connection = createConfig()
          //get table
          val rowDataTable = connection.getTable(TableName.valueOf(testTable))
          val rowData = rowDataTable.get(new Get("testRowID2".getBytes).addFamily(testColMetaDataName.getBytes)).value().toSeq
          //get value for specific rowID
          assertResult(data)(rowData)
          connection.close()
        }

        it("should write data into column qualifier when specified", HBaseTest) {
          val result = hbc.writeMetaData(tableName = testTable, qualifierName = qualifier1, rowID = "testID1", data=data)
          assertResult(true)(result)
        }

        it("should store the data in a column qualifier when specified", HBaseTest) {
          hbc.writeMetaData(tableName = testTable, qualifierName = qualifier2, rowID = "testID", data = data)
          //create connection
          val connection = createConfig()
          //get table
          val rowDataTable = connection.getTable(TableName.valueOf(testTable))
          val rowData = rowDataTable.get(new Get("testID".getBytes).addColumn(testColMetaDataName.getBytes, qualifier2.getBytes)).value().toSeq
          assertResult(data)(rowData)
          connection.close()
        }

        it("return true when non-existent table is given", HBaseTest) {
          assertResult(true)(hbc.writeMetaData(tableName = nonExistentTable3, rowID = "anotherTestRowID", data = data))
        }

        it("should create a table in HBase when specified table does not exist", HBaseTest) {
          //create connection
          val connection = createConfig()
          val isTableExist = connection.getAdmin.tableExists(TableName.valueOf(nonExistentTable4))
          //check if table exists
          assertResult(false)(isTableExist)
          hbc.writeMetaData(tableName = nonExistentTable4, rowID = "anotherTestID", data = data)
          //use connection to see if table exists Now
          val isTableExistNow = connection.getAdmin.tableExists(TableName.valueOf(nonExistentTable4))
          assertResult(true)(isTableExistNow)
          connection.close()
        }
      }
    }
  }
}
