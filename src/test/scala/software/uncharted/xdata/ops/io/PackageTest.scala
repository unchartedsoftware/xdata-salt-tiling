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

import java.io.File
import java.nio.file.{Paths, Files}

import org.apache.commons.io.FileUtils
import software.uncharted.salt.core.generation.output.TestSeriesData
import software.uncharted.salt.core.util.SparseArray
import software.uncharted.xdata.ops.salt.MercatorTimeProjection
import software.uncharted.xdata.spark.SparkFunSpec

import org.apache.hadoop.hbase.client._;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;

import net.liftweb.json.JsonAST.compactRender
import net.liftweb.json.Extraction.decompose



// scalastyle:off magic.number
// scalastyle:off multiple.string.literals
class PackageTest extends SparkFunSpec {

  private val testDir = "build/tmp/test_file_output/test_data"
  private val testLayer = "test_layer"
  private val testBucket = "uncharted-s3-client-test"
  private val extension = ".tst"

  lazy val awsAccessKey = sys.env("AWS_ACCESS_KEY")
  lazy val awsSecretKey = sys.env("AWS_SECRET_KEY")

  lazy val zookeeperQuorum = "uscc0-node08.uncharted.software"
  lazy val zookeeperPort = "2181"
  lazy val hBaseMaster = "hdfs://uscc0-master0.uncharted.software:60000"

  private val configFile = Seq("/home/asuri/Documents/hbase-site.xml")

  def genHeatmapArray(in: Double*) = in.foldLeft(new SparseArray(0, 0.0))(_ += _)
  def genTopicArray(in: List[(String, Int)]*) = in.foldLeft(new SparseArray(0, List[(String, Int)]()))(_ += _)

  implicit val formats = net.liftweb.json.DefaultFormats

  describe("#writeToFile") {
    it("should create the folder directory structure if it's missing") {
      try {
        val data = sc.parallelize(Seq(((1, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))))
        writeToFile(s"${testDir}_1", testLayer, extension)(data)
        val testFile = new File(s"${testDir}_1/$testLayer/01/2")
        assertResult(true)(testFile.exists())
      } finally {
        FileUtils.deleteDirectory(new File(s"${testDir}_1"))
      }
    }

    it("should add tiles to the folder hierarchy using a path based on TMS coords ") {
      try {
        val data = sc.parallelize(Seq(
          ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7)),
          ((2, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
        ))
        writeToFile(s"${testDir}_2", testLayer, extension)(data)
        assertResult(true)(new File(s"${testDir}_2/$testLayer/02/2/2.tst").exists())
        assertResult(true)(new File(s"${testDir}_2/$testLayer/02/2/3.tst").exists())
      } finally {
        FileUtils.deleteDirectory(new File(s"${testDir}_2"))
      }
    }

    it("should serialize the byte array without changing it") {
      try {
        val data = sc.parallelize(Seq(((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))))
        writeToFile(s"${testDir}_3", testLayer, extension)(data)
        val bytes = Files.readAllBytes(Paths.get(s"${testDir}_3/$testLayer/02/2/2.tst"))
        assertResult(Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))(bytes)
      } finally {
        FileUtils.deleteDirectory(new File(s"${testDir}_3"))
      }
    }
  }

  describe("#writeToS3") {
    val testKey0 = s"$testLayer/2/2/2.bin"
    val testKey1 = s"$testLayer/2/2/3.bin"

    it("should add tiles to the s3 bucket using key names based on TMS coords", S3Test) {
      val data = sc.parallelize(Seq(
        ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7)),
        ((2, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
      ))

      writeToS3(awsAccessKey, awsSecretKey, testBucket, testLayer)(data).collect()

      val s3c = new S3Client(awsAccessKey, awsSecretKey)
      assert(s3c.download(testBucket,testKey0).isDefined)
      assert(s3c.download(testBucket, testKey1).isDefined)
      s3c.delete(testBucket, testKey0)
      s3c.delete(testBucket, testKey1)
    }

    it("should serialize the byte data to the s3 bucket without changing it", S3Test) {
      val data = sc.parallelize(Seq(
        ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
      ))

      writeToS3(awsAccessKey, awsSecretKey, testBucket, testLayer)(data)
      val s3c = new S3Client(awsAccessKey, awsSecretKey)
      assertResult(Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))(s3c.download(testBucket, testKey0).getOrElse(fail()))
      s3c.delete(testBucket, testKey0)
    }
  }

  describe("#writeBytesToS3") {
    val testFile = "metadata.json"
    it("should write the byte data to the s3 bucket without changing it", S3Test) {
      writeBytesToS3(awsAccessKey, awsSecretKey, testBucket, testLayer)(testFile, Seq(0, 1, 2, 3, 4, 5))
      val s3c = new S3Client(awsAccessKey, awsSecretKey)
      assertResult(Seq[Byte](0, 1, 2, 3, 4, 5))(s3c.download(testBucket, s"$testLayer/$testFile").getOrElse(fail()))
      s3c.delete(testBucket, s"$testLayer/$testFile")
    }
  }

  describe("#writeToHBase") {
    it("should add tiles to the HBase Table Created", HBaseTest) {
      val data = sc.parallelize(Seq(
        ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7)),
        ((2, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
      ))
      val testCol = "tileData"
      val testQualifier = "tileDataQuali"

      writeToHBase(configFile, testLayer, testQualifier)(data).collect()

      val config = HBaseConfiguration.create()
      config.set("hbase.zookeeper.quorum", zookeeperQuorum)
      config.set("hbase.zookeeper.property.clientPort", zookeeperPort)
      config.set("hbase.master", hBaseMaster)
      config.set("hbase.client.keyvalue.maxsize", "0")
      val connection = ConnectionFactory.createConnection(config)
      val admin = connection.getAdmin
      assertResult(true)(connection.getTable(TableName.valueOf(testLayer)).exists(new Get (s"${testLayer}/02/2/2.bin".getBytes())))
      assertResult(true)(connection.getTable(TableName.valueOf(testLayer)).exists(new Get (s"${testLayer}/02/2/3.bin".getBytes())))
      //disable and delete test table
      admin.disableTable(TableName.valueOf(testLayer))
      admin.deleteTable(TableName.valueOf(testLayer))
      connection.close()
    }

    it("should serialize the byte data to HBase without changing it", HBaseTest) {
      val data = sc.parallelize(Seq(
        ((2, 2, 2), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7)),
        ((2, 2, 3), Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))
      ))

      val testCol = "tileData"
      val testQualifier = "tileDataQuali"
      writeToHBase(configFile, testLayer, testQualifier)(data)
      val config = HBaseConfiguration.create()
      config.set("hbase.zookeeper.quorum", zookeeperQuorum)
      config.set("hbase.zookeeper.property.clientPort", zookeeperPort)
      config.set("hbase.master", hBaseMaster)
      config.set("hbase.client.keyvalue.maxsize", "0")
      val connection = ConnectionFactory.createConnection(config)
      val admin = connection.getAdmin

      assertResult(Seq[Byte](0, 1, 2, 3, 4, 5, 6, 7))(connection.getTable(TableName.valueOf(testLayer)).get(new Get(s"${testLayer}/02/2/2.bin".getBytes).addFamily(testCol.getBytes)).value().toSeq)

      admin.disableTable(TableName.valueOf(testLayer))
      admin.deleteTable(TableName.valueOf(testLayer))
      connection.close()
    }
  }

  describe("#writeBytesToHBase") {
    val testFile = "metadata.json"
    val testQualifier = "tileQualifier"
    it("should write the byte data to the HBaseTable without changing it", HBaseTest) {
      writeBytesToHBase(configFile, testLayer, testQualifier)(testFile, Seq(0, 1, 2, 3, 4, 5))
      val hbc = HBaseConnector(configFile)
      //disable and delete table
      val config = HBaseConfiguration.create()
      config.set("hbase.zookeeper.quorum", zookeeperQuorum)
      config.set("hbase.zookeeper.property.clientPort", zookeeperPort)
      config.set("hbase.master", hBaseMaster)
      config.set("hbase.client.keyvalue.maxsize", "0")
      val connection = ConnectionFactory.createConnection(config)
      val admin = connection.getAdmin

      assertResult(true)(connection.getTable(TableName.valueOf(testLayer)).exists(new Get (s"${testLayer}/${testFile}".getBytes())))
      admin.disableTable(TableName.valueOf(testLayer))
      admin.deleteTable(TableName.valueOf(testLayer))
      connection.close()

    }
  }

  describe("#serializeBinArray") {
    val arr0 = genHeatmapArray(0.0, 1.0, 2.0, 3.0)
    val arr1 = genHeatmapArray(4.0, 5.0, 6.0, 7.0)

    it("should create an RDD of bin coordinate / byte array tuples from series data") {
      val series = sc.parallelize(
        Seq(
          TestSeriesData(new MercatorTimeProjection(Seq(0)), (3, 1, 1), (1, 2, 3), arr0, Some(0.0, 1.0)),
          TestSeriesData(new MercatorTimeProjection(Seq(0)), (3, 1, 1), (4, 5, 6), arr1, Some(0.0, 1.0))
        ))
      val result = serializeBinArray(series).collect()
      assertResult(2)(result.length)
      assertResult((1, 2, 3))(result(0)._1)
      assertResult(
        List(
          0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, -16, 63,
          0, 0, 0, 0, 0, 0, 0, 64,
          0, 0, 0, 0, 0, 0, 8, 64))(result(0)._2)

      assertResult((4, 5, 6))(result(1)._1)
      assertResult(
        List(
          0, 0, 0, 0, 0, 0, 16, 64,
          0, 0, 0, 0, 0, 0, 20, 64,
          0, 0, 0, 0, 0, 0, 24, 64,
          0, 0, 0, 0, 0, 0, 28, 64))(result(1)._2)
    }
  }

  describe("#mkRowId") {
    it("should form proper IDs without suffix or prefix") {
      assertResult("04,07,03")(mkRowId("", ",", "")(4, 7, 3))
      assertResult("03:1:3")(mkRowId("", ":", "")(3, 1, 3))
      assertResult("10--0045--0110")(mkRowId("", "--", "")(10,45,110))
    }
    it("should prepend and append prefix and suffix properly") {
      assertResult("abc/04/07/03.bin")(mkRowId("abc/", "/", ".bin")(4, 7, 3))
      assertResult("abc/03:1:3.bin")(mkRowId("abc/", ":", ".bin")(3, 1, 3))
      assertResult("abc/10--0045--0110.bin")(mkRowId("abc/", "--", ".bin")(10,45,110))
    }
  }

  describe("#serializeBinArray") {
    it("should ignore tiles with no updated bins") {
      val series = sc.parallelize(
        Seq(TestSeriesData(
          new MercatorTimeProjection(Seq(0)), (3, 1, 1), (1, 2, 3), genHeatmapArray(0.0, 0.0, 0.0, 0.0), Some(0.0, 1.0))))
      val result = serializeBinArray(series).collect()
      assertResult(0)(result.length)
    }
  }

  // Alternative reference version against which to test.
  val canonicalDoubleTileToByteArrayDense: SparseArray[Double] => Seq[Byte] = sparseData => {
    for (bin <- sparseData.seq;  i <- 0 until doubleBytes) yield {
      val datum = java.lang.Double.doubleToLongBits(bin);
      ((datum >> (i * doubleBytes)) & 0xff).asInstanceOf[Byte];
    }
  }

  describe("#doubleTileToByteDenseArray") {
    it("should be the same in version 1 and 2") {
      val data = genHeatmapArray(1.0, 2.0, 3.0, 4.0)
      val ba1 = canonicalDoubleTileToByteArrayDense(data)
      val ba2 = doubleTileToByteArrayDense(data)
      assertResult(ba1.length)(ba2.length)
      for (i <- ba1.indices) assertResult(ba1(i))(ba2(i))
    }
  }

  describe("#serializeElementScore") {
    it("should create an RDD of JSON strings serialized to bytes from series data ") {
      val arr0 = genTopicArray(List("aa" -> 1, "bb" -> 2))
      val arr1 = genTopicArray(List("cc" -> 3, "dd" -> 4))

      val series = sc.parallelize(
        Seq(
          TestSeriesData(new MercatorTimeProjection(Seq(0)), (1, 1, 1), (1, 2, 3), arr0, None),
          TestSeriesData(new MercatorTimeProjection(Seq(0)), (1, 1, 1), (4, 5, 6), arr1, None)
        ))

      val json = List(compactRender(decompose((Map("aa" -> 1, "bb" -> 2)))).toString().getBytes, compactRender(decompose(Map("cc" -> 3, "dd" -> 4))).toString().getBytes)

      val result = serializeElementScore(series).collect()
      assertResult(2)(result.length)

      assertResult(result(0)._1)((1, 2, 3))
      assertResult(json.head)(result(0)._2)

      assertResult(result(1)._1)((4, 5, 6))
      assertResult(json(1))(result(1)._2)
    }
  }

  describe("#serializeElementScore") {
    it("should ignore tiles with no updated bins ") {
      val series = sc.parallelize(
        Seq(TestSeriesData(new MercatorTimeProjection(
          Seq(0)), (1, 1, 1), (1, 2, 3), genTopicArray(List()), None)))
      val result = serializeElementScore(series).collect()
      assertResult(0)(result.length)
    }
  }
}
