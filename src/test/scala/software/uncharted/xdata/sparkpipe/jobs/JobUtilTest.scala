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

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.scalatest.{BeforeAndAfterAll, Tag}
import software.uncharted.sparkpipe.ops.xdata.io.S3Client
import software.uncharted.xdata.spark.SparkFunSpec
import software.uncharted.xdata.tiling.jobs.JobUtil.{createMetadataOutputOperation, createTileOutputOperation}

import scala.util.Failure

object S3Test extends Tag("s3.test")
object HBaseTest extends Tag("hbc.test")

class JobUtilTest extends SparkFunSpec with BeforeAndAfterAll {

  lazy private val awsAccessKey = sys.env.getOrElse("AWS_ACCESS_KEY", throw new Exception("AWS_ACCESS_KEY is unset"))
  lazy private val awsSecretKey = sys.env.getOrElse("AWS_SECRET_KEY0", throw new Exception("AWS_SECRET_KEY is unset"))

  private val testBucket = "uncharted-s3-test"
  private val s3Layer = "test_layer"

  lazy private val zookeeperQuorum = sys.env.getOrElse("HBASE_ZOOKEEPER_QUORUM", throw new Exception("HBASE_ZOOKEEPER_QUORUM is unset"))
  lazy private val zookeeperPort = sys.env.getOrElse("HBASE_ZOOKEEPER_CLIENTPORT", "2181")
  lazy private val hBaseMaster = sys.env.getOrElse("HBASE_MASTER", throw new Exception("HBASE_MASTER is unset"))
  lazy private val hBaseClientKeyValMaxSize = sys.env.getOrElse("HBASE_CLIENT_KEYVALUE_MAXSIZE", "0")
  private val hbaseLayer = "testTable"

  private val configFile = Seq(classOf[JobUtilTest].getResource("/hbase-site.xml").toURI.getPath)

  private val s3config = ConfigFactory.parseString(
    "s3Output.awsAccessKey = ${?AWS_ACCESS_KEY}\n" +
    "s3Output.awsSecretKey = ${?AWS_SECRET_KEY}\n" +
    s"s3Output.bucket = $testBucket\n" +
    s"s3Output.layer = $s3Layer\n" +
    "s3Output.ext = bin"
  ).resolve()

  private val hbaseConfig = ConfigFactory.parseString(
    s"""hbaseOutput.configFiles = ["$configFile"]\n""" +
    s"hbaseOutput.layer = $hbaseLayer\n" +
    """hbaseOutput.qualifier = """""
    ).resolve()

  private val dataSeq = Seq(
    ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7)),
    ((2, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
  )

  private val testFile = "metadata.json"
  private val jsonBytes = Seq[Byte](0, 1, 2, 3, 4, 5)

  lazy private val s3c = new S3Client(awsAccessKey, awsSecretKey)

  protected override def beforeAll() = {
    s3c.createBucket(testBucket)
  }

  protected override def afterAll() = {
    s3c.deleteBucket(testBucket)
  }

  describe("JobUtilTest") {

    it("should throw an exception when config is invalid", S3Test) {
      val config = ConfigFactory.empty()
      val tileOpResult = createTileOutputOperation(config)
      val tilingError = "No output operation given"

      assertResult(tilingError)(tileOpResult.asInstanceOf[Failure[Exception]].exception.getLocalizedMessage)

      val metadataOpResult = createMetadataOutputOperation(config)
      val metadataError = "No metadata output operation given"

      assertResult(metadataError)(metadataOpResult.asInstanceOf[Failure[Exception]].exception.getLocalizedMessage)
    }

    describe("#createTileOutputOperation") {
      it("should create a writeToS3 function when it is a s3 config file", S3Test) {

                s3c.createBucket(testBucket)

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

      it("should create a writeToHBase function when it is a hbase config file", HBaseTest) {
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

    describe("#createMetadataOutputOperation") {
      it("should write metadata to s3", S3Test) {
        val outputOp = createMetadataOutputOperation(s3config).get
        outputOp(testFile, jsonBytes)

        try {
          assertResult(Seq[Byte](0, 1, 2, 3, 4, 5))(s3c.download(testBucket, s"$s3Layer/$testFile").getOrElse(fail()))
        } finally {
          s3c.delete(testBucket, s"$s3Layer/$testFile")
        }
      }

      it("should write metadata to hbase ", HBaseTest) {
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
