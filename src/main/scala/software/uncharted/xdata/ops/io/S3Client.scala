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

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{PutObjectRequest, CannedAccessControlList}
import grizzled.slf4j.Logging

object S3Client {
  def apply(accessKey: String, secretKey: String): S3Client = new S3Client(accessKey, secretKey)
}

class S3Client(accessKey: String, secretKey: String) extends Logging {

  val s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey))

  def createBucket(bucketName: String): Boolean = {
    try {
      // TODO: Make CORS support an argument, enable static web serving
      if (!s3Client.doesBucketExist(bucketName)) { s3Client.createBucket(bucketName); true } else false
    } catch {
      case e: Exception => error(s"Failed to create bucket $bucketName"); false // scalastyle:ignore
    }
  }

  def deleteBucket(bucketName: String): Boolean = {
    try {
      if (s3Client.doesBucketExist(bucketName)) { s3Client.deleteBucket(bucketName); true } else false
    } catch {
      case e: Exception => error(s"Failed to delete bucket $bucketName"); false // scalastyle:ignore
    }
  }

  def delete(bucketName: String, key: String): Boolean = {
    try {
      s3Client.deleteObject(bucketName, key); true
    } catch {
      case e: Exception => error(s"Failed to delete $key", e); false
    }
  }

  def upload(data: Seq[Byte], bucketName: String, key: String): Boolean = {
    val bos = new ByteArrayInputStream(data.toArray)
    try {
      s3Client.putObject(new PutObjectRequest(bucketName, key, bos, null) // scalastyle:ignore
        .withCannedAcl(CannedAccessControlList.PublicRead))
      true
    } catch {
      case e: Exception => error(s"Failed to upload $key", e); false
    }
  }

  def download(bucketName: String, key: String): Option[Seq[Byte]]= {
    // Iteratively read chunks of the input stream into a fixed size buffer and write the
    // results out to a byte array output stream.
    val bufferSize = 8192
    val buffer = Array.ofDim[Byte](bufferSize)
    val bos = new ByteArrayOutputStream()
    try {
      val dis = s3Client.getObject(bucketName, key).getObjectContent
      var bytesRead =  dis.read(buffer)
      while (bytesRead != -1) {
        bos.write(buffer, 0, bytesRead)
        bytesRead = dis.read(buffer)
      }
      Some(bos.toByteArray.toSeq)
    } catch {
      case e: Exception => error(s"Failed to download $key", e); None
    }
  }
}
