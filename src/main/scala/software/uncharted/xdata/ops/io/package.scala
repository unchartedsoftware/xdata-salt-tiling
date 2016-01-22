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
package software.uncharted.xdata.ops

import java.io.{BufferedOutputStream, FileOutputStream, File}

import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import software.uncharted.salt.core.generation.output.SeriesData

import scala.util.parsing.json.JSONObject

package object io extends Logging {

  val doubleBytes = 8

  /**
   * Write binary array data to the file system.  Folder structure is
   * baseFilePath/layerName/level/xIdx/yIdx.bin.  Indexing is TMS style
   * with (0,0) in lower left, y increasing as it moves north.
   *
   * @param baseFilePath Baseline output directory - can store multiple layers.
   * @param layerName Unique name for the layer .
   * @param input tile coordinate / byte array tuples of tile data
   * @return input data unchanged
   */
  def writeToFile(baseFilePath: String, layerName: String, extension: String)(input: RDD[((Int, Int, Int), Seq[Byte])]): RDD[((Int, Int, Int), Seq[Byte])] = {
    input.collect().foreach { tileData =>
      val coord = tileData._1
      val dirPath = s"$baseFilePath/$layerName/${coord._1}/${coord._2}" //scalastyle:ignore
      val path = s"$dirPath/${coord._3}.$extension"
      try {
        // create path if necessary
        val file = new File(dirPath)
        file.mkdirs()
        val fos = new FileOutputStream(new File(path))
        val bos = new BufferedOutputStream(fos)
        bos.write(tileData._2.toArray)
        bos.close()
      } catch {
        case e: Exception => error(s"Failed to write file $path", e)
      }
    }
    input
  }

  /**
   * Write binary array data to Amazon S3 bucket.  Key format is layerName/level-xIdx-yIdx.bin.
   * Indexing is TMS style with (0,0) in lower left, y increasing as it moves north
   *
   * @param layerName Unique name for the layer .
   * @param input tile coordinate / byte array tuples of tile data
   * @return input data unchanged
   */
  def writeToS3(accessKey: String, secretKey: String, bucketName: String, layerName: String)(input: RDD[((Int, Int, Int), Seq[Byte])]):
  RDD[((Int, Int, Int), Seq[Byte])] = {
    // Upload tiles to S3 using the supplied bucket and layer.  Use foreachPartition to avoid incurring
    // the cost of initializing the S3Client per record.  This can't be done outside the RDD closure
    // because the Amazon S3 API classes are not marked serializable.
    input.foreachPartition { tileDataIter =>
      val s3Client = S3Client(accessKey, secretKey)
      tileDataIter.foreach { tileData =>
        val coord = tileData._1
        // store tile in bucket as layerName/level-xIdx-yIdx.bin
        val key = s"$layerName/${coord._1}/${coord._2}/${coord._3}.bin"
        s3Client.upload(tileData._2.toArray, bucketName, key)
      }
    }
    input
  }

  def serializeBinArray(tiles: RDD[SeriesData[(Int, Int, Int), Double, (Double, Double)]]): RDD[((Int, Int, Int), Seq[Byte])] = {
    tiles.filter(t => t.binsTouched > 0)
      .map { tile =>
        val data = for (bin <- tile.bins; i <- 0 until doubleBytes) yield {
          val data = java.lang.Double.doubleToLongBits(bin)
          ((data >> (i * doubleBytes)) & 0xff).asInstanceOf[Byte]
        }
        (tile.coords, data)
      }
  }

  def serializeElementScore(tiles: RDD[SeriesData[(Int, Int, Int), List[(String, Int)], Nothing]]): RDD[((Int, Int, Int), Seq[Byte])] = {
    tiles.filter(t => t.binsTouched > 0)
      .map { t =>
        val bytes = new JSONObject(t.bins.head.toMap).toString().getBytes
        (t.coords, bytes.toSeq)
      }
  }
}
