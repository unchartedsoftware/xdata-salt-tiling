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

object HBaseConnectorTest extends Tag("hBase.test")

class HBaseConnectorTest extends SparkFunSpec with BeforeAndAfterAll {

  protected override def beforeAll() = {
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "uscc0-node08.uncharted.software")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.master", "hdfs://uscc0-master0.uncharted.software:60000")
    config.set("hbase.client.keyvalue.maxsize", "0")
    val connection = ConnectionFactory.createConnection(config)
    val admin = connection.getAdmin

    List(existingTableName, testTable2Name).foreach { tableName =>
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      tableDescriptor.addFamily(new HColumnDescriptor(testColName))
      admin.createTable(tableDescriptor)
    }
    existingTable = connection.getTable(TableName.valueOf(existingTableName))
    testTable2 = connection.getTable(TableName.valueOf(testTable2Name))

    admin.disableTable(TableName.valueOf(testTable2Name))
    admin.deleteTable(TableName.valueOf(testTable2Name))
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

    existingTable.close
    testTable2.close

    admin.disableTable(TableName.valueOf(existingTableName))
    admin.disableTable(TableName.valueOf(testTableName))
    admin.disableTable(TableName.valueOf(testTable2Name))
    admin.deleteTable(TableName.valueOf(existingTableName))
    admin.deleteTable(TableName.valueOf(testTableName))
    admin.deleteTable(TableName.valueOf(testTable2Name))
    connection.close()

    hb.close
  }

  private lazy val hb = HBaseConnector("uscc0-node08.uncharted.software", "2181", "hdfs://uscc0-master0.uncharted.software:60000")

  var existingTable: Table = _
  var testTable2: Table = _

  private val testColName = "testCol"
  private val nonExistantColName = "nonExistantCol"

  private val testTableName = "testTable"
  private val testTable2Name = "testTable2"
  private val existingTableName = "existingTable"
  private val nonExistantTableName = "nonExistantTable"

  private val data = Seq[Byte](0, 1, 2, 3, 4, 5)

  private val data2 = Array(("3", data.toSeq), ("4", data.toSeq))
  private val data3 = Array(("5", data.toSeq), ("6", data.toSeq))

  describe("HBaseConnectorTest") {

    describe("#createTable") {
      it("should return true on table creation") {
        val result = hb.createTable(testTableName, testColName)
        assertResult(true)(result)
      }
      it("should return false when trying to create a table that already exists") {
        val result = hb.createTable(existingTableName, testColName)
        assertResult(false)(result)
      }
      it("should create a table in HBase when true is returned") {
        val result = hb.createTable(testTable2Name, testColName)
        val table = hb.getTable(testTable2Name).map(item => item.getName().getNameAsString())
        assertResult(Some(testTable2Name))(table)
      }
    }
    describe("#writeRow") {
      it("should return true on row insertion into table+column") {
        val result = hb.writeRow(existingTableName, testColName, "1", data)
        assertResult(true)(result)
      }
      it("should store the data in the specified table+column in HBase") {
        hb.writeRow(existingTableName, testColName, "2", data)
        val rowDataTable = hb.getTable(existingTableName)
        val rowData = rowDataTable.get.get(new Get("2".getBytes).addFamily(testColName.getBytes)).value().toSeq
        assertResult(data)(rowData)
      }
      it("should return false if column does not exist") {
        val result = hb.writeRow(existingTableName, nonExistantColName, "2", data)
        assertResult(false)(result)
      }
    }
    describe("#writeRows") {
      it("should return true when rows are written to table") {
        //can't have private vals in unit test?
        val rddData2: RDD[(String, Seq[Byte])] = sc.parallelize(data2)

        val result = hb.writeRows(testTableName, "testCol", rddData2)
        assertResult(true)(result)
      }
      it("should return false when provided with a column that is not in the table") {
        val rddData2: RDD[(String, Seq[Byte])] = sc.parallelize(data2)

        val result = hb.writeRows(testTableName, "nonExistantCol", rddData2)
        assertResult(false)(result)
      }
      it("should write the rows into HBase") {
        val rddData3: RDD[(String, Seq[Byte])] = sc.parallelize(data3)

        hb.writeRows(testTableName, "testCol", rddData3)
        val table = hb.getTable(testTableName).get
        List(("5", data), ("6", data)).foreach { row =>
          val rowData = table.get(new Get(row._1.getBytes()).addFamily("testCol".getBytes())).getValue("testCol".getBytes(), Array[Byte]())
          assertResult(row._2)(rowData)
        }
      }
    }
    describe("#getTable") {
      it("should return a table instance") {
        val result = hb.getTable(existingTableName).map(item => item.getName().getNameAsString())
        assertResult(Some(existingTableName))(result)
      }
      it("should return none if table doesn't exist") {
        val result = hb.getTable(nonExistantTableName)
        assertResult(None)(result)
      }
    }
    describe("#createConnection") {
      it ("should create a connection object given the correct parameters") {
          val HBaseObjectTest = HBaseConnector("uscc0-node08.uncharted.software", "2181", "hdfs://uscc0-master0.uncharted.software:60000")
          HBaseObjectTest.close
          assertResult("software.uncharted.xdata.ops.io.HBaseConnector")(HBaseObjectTest.getClass.getName)
      }
    }
    describe("#initTableIfNeeded"){
      it("should create a table on connector instantiation, when passed in, for a non existant HBase Table"){
        val hbc = HBaseConnector("uscc0-node08.uncharted.software", "2181", "hdfs://uscc0-master0.uncharted.software:60000", Some("testTableToBeCreated"), Some("testCol"))
        val tableName = hbc.getTable("testTableToBeCreated").get.getName().getNameAsString()
        hbc.close
        assertResult("testTableToBeCreated")(tableName)
      }
      it("should not create a table when no table arguments are passed in to the constructor"){
        val hbc = HBaseConnector("uscc0-node08.uncharted.software", "2181", "hdfs://uscc0-master0.uncharted.software:60000")
        val tableName = hbc.getTable("tableThatWasntCreated")
        hbc.close
        assertResult(None)(tableName)
      }

    }
  }

}
