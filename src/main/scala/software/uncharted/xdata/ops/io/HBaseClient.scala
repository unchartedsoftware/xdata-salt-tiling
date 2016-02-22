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

//import byte streaming libraries
import java.io.{ByteArrayOutputStream, ByteArrayInputStream, InputStream}
import java.util.zip.{GZIPOutputStream, GZIPInputStream}

//for logging, aperture tiles uses org.apache.log4j for logging
import grizzled.slf4j.Logging

//import needed HBase libraries
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put, Table};
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf

import java.net.URI
import org.apache.hadoop.fs.Path

// object HBaseConnector {
//
//   def apply(zookeeperQuorum: String, zookeeperPort: String, hBaseMaster: String, tableName: Option[String] = None, colName: Option[String] = None) : HBaseConnector = {
//     new HBaseConnector(zookeeperQuorum, zookeeperPort, hBaseMaster, tableName, colName)
//   }
// }
//
// class HBaseConnector(zookeeperQuorum: String, zookeeperPort: String, hBaseMaster: String, initTableName: Option[String], initColName: Option[String]) extends Logging {
//   private val connection = createConnection(zookeeperQuorum, zookeeperPort, hBaseMaster)
//   private val admin = connection.getAdmin()
//   initTableIfNeeded(initTableName, initColName)
//
//   def createTable(tableName: String, colName: String): Boolean = {
//     try {
//       if (!admin.tableExists(TableName.valueOf(tableName))) {
//         val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
//         tableDescriptor.addFamily(new HColumnDescriptor(colName))
//         admin.createTable(tableDescriptor)
//         true
//       } else { error(s"$tableName already exists"); false }
//     } catch {
//       case e: Exception => error(s"Error while creating table"); false
//     }
//   }
//
//   def writeRow(tableName: String, colName: String, rowID: String, data: Seq[Byte]): Boolean = {
//     try {
//       val table = this.getTable(tableName)
//       List(table).flatMap(item => item.map(_.put(new Put(rowID.getBytes).addColumn(colName.getBytes, Array[Byte](), data.toArray))))
//       val checkEmpty = table.map(_.close)
//       val rowsWritten = if (checkEmpty.isEmpty) false else true
//       rowsWritten
//     } catch {
//       case e: Exception => error(s"Failed to write row into table"); false
//     }
//   }
//
//   def writeRows(tableName: String, colName: String, listOfRowInfo: RDD[(String, Seq[Byte])]): Boolean = {
//     try {
//       val putList = listOfRowInfo.map { rowInfo =>
//         (new ImmutableBytesWritable, new Put(rowInfo._1.getBytes).addColumn(colName.getBytes, Array[Byte](), rowInfo._2.toArray))
//       }
//       val jobConfig = new JobConf(getConfig(), this.getClass)
//       jobConfig.setOutputFormat(classOf[TableOutputFormat])
//       jobConfidef close: Unit = admin.close()g.set(TableOutputFormat.OUTPUT_TABLE, tableName)
//       putList.saveAsHadoopDataset(jobConfig)
//       true
//     } catch {
//       case e: Exception => error(s"Failed to write into table $tableName"); false
//     }
//   }
//
//
//   def getTable(tableName: String): Option[Table] = {
//     try {
//       if(admin.tableExists(TableName.valueOf(tableName))) {
//         Some(connection.getTable(TableName.valueOf(tableName)))
//       }
//       else {
//         None
//       }
//     } catch {
//       case e: Exception => error(s"table doesn't exist $tableName"); None
//     }
//   }
//
//   def close: Unit = admin.close()
//
//   private def createConnection(zookeeperQuorum: String, zookeeperPort: String, hBaseMaster: String): Connection = {
//     val config = getConfig()
//     ConnectionFactory.createConnection(config)
//   }
//
//   private def getConfig(): Configuration = {
//     val config = HBaseConfiguration.create()
//     config.set("hbase.zookeeper.quorum", zookeeperQuorum)
//     config.set("hbase.zookeeper.property.clientPort", zookeeperPort)
//     config.set("hbase.master", hBaseMaster)
//     config.set("hbase.client.keyvalue.maxsize", "0")
//     config
//   }Array[Byte]()
//       }
//     }
//   }
//
// }

object HBaseConnector {
  def apply(configFile: Seq[String]) : HBaseConnector = {
    new HBaseConnector(configFile)
  }
}

class HBaseConnector(configFile: Seq[String]) extends Logging {
  private val hbaseConfiguration = HBaseConfiguration.create()
  configFile.foreach{configFileItem =>
  hbaseConfiguration.addResource(new Path(new URI(configFileItem)))
  }
  private val connection = getConnection(hbaseConfiguration)
  private val admin = connection.getAdmin

  private val colFamilyData = "tileData"
  private val colFamilyMetaData = "tileMetaData"


  //writeRows is per family/qualifier set or do we want to include arrays of different colfamily/data pairs?
  def writeRows(tableName: String, colFamilyName: String, qualifierName: String = "")(listOfRowInfo: RDD[(String, Seq[Byte])]): Boolean = {
    //check if table is there
    //I don't get the option convention..if I get a None, I can't map the collection because you can't call the methods on a None)
    tableExistsOrCreate(tableName)
    //check if col name is there
    //colFamilyExistsOrCreate(tableName, colFamilyName)
    //write the rows into column Family
    try {
      val putList = listOfRowInfo.map { rowInfo =>
        (new ImmutableBytesWritable, new Put(rowInfo._1.getBytes).addColumn(colFamilyName.getBytes, qualifierName.getBytes, rowInfo._2.toArray))
      }
      val jobConfig = new JobConf(hbaseConfiguration, this.getClass)
      jobConfig.setOutputFormat(classOf[TableOutputFormat])
      jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      putList.saveAsHadoopDataset(jobConfig)
      true
    } catch {
      case e: Exception => error(s"Failed to write into table $tableName"); false
    }
  }

  // def writeRow(tableName: String, colName: String, rowID: String, data: Seq[Byte]): Boolean = {
  //   try {
  //     val table = connection.getTable(tableName)
  //     List(table).flatMap(item => item.map(_.put(new Put(rowID.getBytes).addColumn(colName.getBytes, Array[Byte](), data.toArray))))
  //     val checkEmpty = table.map(_.close)
  //     val rowsWritten = if (checkEmpty.isEmpty) false else true
  //     rowsWritten
  //   } catch {
  //     case e: Exception => error(s"Failed to write row into table"); false
  //   }
  // }

  def close: Unit = admin.close()


  //is this good practice? it technically does two things.
  //TODO. CHECK IF TABLE EXISTS.
  //IF TABLE DOESN'T EXIST: CREATE TABLE
  //THEN GET TABLE AFTER THE IF STATEMENT
  //^ADD THAT IN WRITE ROWS
  //Do I even need an option here? i'll try to create if it fails then i'll return an error..
  //but I can't return an error if my return value is Table..so I think I do need an option.
  private def tableExistsOrCreate(tableName: String): Boolean = {
    try {
      if(!admin.tableExists(TableName.valueOf(tableName))) {
        createTable(tableName)
      }
      else{
        true
      }
    } catch {
      case e: Exception => error(s"couldn't return table: $tableName"); false
    }
  }

  // //TODO: this should follow the same TODO format as tableExistsOrCreate.
  // //interfaces promote polymorphism. can't use polymorphism to use similar exists.
  // //figure out a way to either abstract these checks or simplify code
  // private def colFamilyExistsOrCreate(tableName: String, colFamilyName: String): Boolean = {
  //   try {
  //     val table = connection.getTable(TableName.valueOf(tableName))
  //     //there's also a getFamily method, but I dk what happens when you pass in a non existant column family name
  //     val tableDescriptor = table.getTableDescriptor()
  //     var checkName = false
  //     tableDescriptor.getColumnFamilies().foreach(item => if(item.getNameAsString() == colFamilyName){ checkName = true})
  //     if (checkName == false) {
  //       //tableDescriptor.addFamily(new HColumnDescriptor(colFamilyName))
  //       admin.addColumn(TableName.valueOf(tableName), new HColumnDescriptor(colFamilyName))
  //     }
  //     true
  //   } catch {
  //     case e: Exception => error(s"colFamily couldn't be added to: $tableName"); false
  //   }
  // }

  private def createTable(tableName: String): Boolean = {
    try {
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      tableDescriptor.addFamily(new HColumnDescriptor(colFamilyData))
      tableDescriptor.addFamily(new HColumnDescriptor(colFamilyMetaData))
      admin.createTable(tableDescriptor)
      true
    } catch {
      case e: Exception => error(s"Error while creating table"); false
    }
  }

  private def getConnection(hbaseConfiguration: Configuration): Connection = {
    ConnectionFactory.createConnection(hbaseConfiguration)
  }
}
