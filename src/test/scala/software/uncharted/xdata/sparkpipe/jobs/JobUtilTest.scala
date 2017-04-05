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
package software.uncharted.xdata.sparkpipe.jobs

import scala.util.Failure
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.scalatest.BeforeAndAfterAll
import software.uncharted.xdata.ops.io.S3Client
import software.uncharted.xdata.spark.SparkFunSpec

class JobUtilTest extends SparkFunSpec with BeforeAndAfterAll{
  import software.uncharted.xdata.sparkpipe.jobs.JobUtil._

  private val awsAccessKey = sys.env("AWS_ACCESS_KEY")
  private val awsSecretKey = sys.env("AWS_SECRET_KEY")
  private val testBucket = "s3-test"
  private val s3Layer = "test_layer"

  private val zookeeperQuorum = sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", throw new Exception("hbase.zookeeper.quorum is unset"))
  private val zookeeperPort = sys.env.getOrElse("HBASE_ZOOKEEPER_CLIENTPORT", "2181")
  private val hBaseMaster = sys.env.getOrElse("HBASE_MASTER", throw new Exception("hbase.master is unset"))
  private val hBaseClientKeyValMaxSize = sys.env.getOrElse("HBASE_CLIENT_KEYVALUE_MAXSIZE", "0")
  private val hbaseLayer = "testTable"

  private val configFile = Seq(classOf[JobUtilTest].getResource("/hbase-site.xml").toURI.getPath)

  private val s3config = ConfigFactory.parseString(
    """s3Output.awsAccessKey = ${?AWS_ACCESS_KEY}
      |s3Output.awsSecretKey = ${?AWS_SECRET_KEY}
      |s3Output.bucket = $testBucket
      |s3Output.layer = $s3Layer
      |s3Output.ext = bin
     """.stripMargin).resolve()

  private val hbaseConfig = ConfigFactory.parseString(
    s"""hbaseOutput.configFiles = ["$configFile"]
       |hbaseOutput.layer = $hbaseLayer
       |hbaseOutput.qualifier = ""
    """.stripMargin).resolve()

  private val dataSeq = Seq(
    ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7)),
    ((2, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
  )

  private val testFile = "metadata.json"
  private val jsonBytes = Seq[Byte](0, 1, 2, 3, 4, 5)

  private val s3c = new S3Client(awsAccessKey, awsSecretKey)

  protected override def beforeAll() = {
    s3c.createBucket(testBucket)
  }

  protected override def afterAll() = {
    s3c.deleteBucket(testBucket)
  }

  describe("#test utility functions for running jobs") {

    it("should throw an exception when config is invalid") {
      val config = ConfigFactory.empty()
      val tileOpResult = createTileOutputOperation(config)
      val tilingError = "No output operation given"

      assertResult(tilingError)(tileOpResult.asInstanceOf[Failure[Exception]].exception.getLocalizedMessage)

      val metadataOpResult = createMetadataOutputOperation(config)
      val metadataError = "No metadata output operation given"

      assertResult(metadataError)(metadataOpResult.asInstanceOf[Failure[Exception]].exception.getLocalizedMessage)
    }

    describe("test createTileOutputOperation functionality") {

      it("should create a writeToS3 function when it is a s3 config file") {
        val data = sc.parallelize(dataSeq)
        val outputOp = createTileOutputOperation(s3config).get

        outputOp(data)

        val testKey0 = s"$s3Layer/02/2/2.bin"
        val testKey1 = s"$s3Layer/02/2/3.bin"

        assert(s3c.download(testBucket,testKey0).isDefined)
        assert(s3c.download(testBucket, testKey1).isDefined)

        s3c.delete(testBucket, testKey0)
        s3c.delete(testBucket, testKey1)
      }

      it("should create a writeToHBase function when it is a hbase config file") {
        val data = sc.parallelize(dataSeq)
        val outputOp = createTileOutputOperation(hbaseConfig).get

        outputOp(data)

        val config = HBaseConfiguration.create()
        config.set("hbase.zookeeper.quorum", zookeeperQuorum)
        config.set("hbase.zookeeper.property.clientPort", zookeeperPort)
        config.set("hbase.master", hBaseMaster)
        config.set("hbase.client.keyvalue.maxsize", hBaseClientKeyValMaxSize)
        val connection = ConnectionFactory.createConnection(config)
        val admin = connection.getAdmin

        assertResult(true)(connection.getTable(TableName.valueOf(hbaseLayer)).exists(new Get ("02,2,2".getBytes())))
        assertResult(true)(connection.getTable(TableName.valueOf(hbaseLayer)).exists(new Get ("02,2,3".getBytes())))

        //disable and delete test table
        admin.disableTable(TableName.valueOf(hbaseLayer))
        admin.deleteTable(TableName.valueOf(hbaseLayer))
        connection.close()
      }

    }

    describe("test createMetadataOutputOperation functionality") {
      it("should write metadata to s3") {
        val outputOp = createMetadataOutputOperation(s3config).get
        outputOp(testFile, jsonBytes)

        try {
          assertResult(Seq[Byte](0, 1, 2, 3, 4, 5))(s3c.download(testBucket, s"$s3Layer/$testFile").getOrElse(fail()))
        } finally {
          s3c.delete(testBucket, s"$s3Layer/$testFile")
        }
      }

      it("should write metadata to hbase ") {
        val outputOp = createMetadataOutputOperation(hbaseConfig).get
        outputOp(testFile, jsonBytes)

        val config = HBaseConfiguration.create()
        config.set("hbase.zookeeper.quorum", zookeeperQuorum)
        config.set("hbase.zookeeper.property.clientPort", zookeeperPort)
        config.set("hbase.master", hBaseMaster)
        config.set("hbase.client.keyvalue.maxsize", hBaseClientKeyValMaxSize)
        val connection = ConnectionFactory.createConnection(config)
        val admin = connection.getAdmin
        val resultTable = connection.getTable(TableName.valueOf(hbaseLayer))
        val getVal = new Get(s"${hbaseLayer}/${testFile}".getBytes())

        try {
          val byteResult = resultTable.get(getVal).value()
          assertResult(true)(resultTable.exists(getVal))
          assertResult(jsonBytes)(byteResult)
        } finally {
          admin.disableTable(TableName.valueOf(hbaseLayer))
          admin.deleteTable(TableName.valueOf(hbaseLayer))
          connection.close()
        }
      }

    }
  }
}
