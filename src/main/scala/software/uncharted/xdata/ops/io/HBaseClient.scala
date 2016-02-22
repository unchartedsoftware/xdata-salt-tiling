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

  def writeRows(tableName: String, colFamilyName: String, qualifierName: String = "")(listOfRowInfo: RDD[(String, Seq[Byte])]): Boolean = {
    tableExistsOrCreate(tableName)
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

  def writeRow(tableName: String, colName: String, rowID: String, data: Seq[Byte]): Boolean = {
    try {
      tableExistsOrCreate(tableName)
      val table = connection.getTable(TableName.valueOf(tableName))
      table.put(new Put(rowID.getBytes).addColumn(colName.getBytes, Array[Byte](), data.toArray))
      val checkEmpty = table.close
      true
    } catch {
      case e: Exception => error(s"Failed to write row into table"); false
    }
  }

  def close: Unit = admin.close()

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
