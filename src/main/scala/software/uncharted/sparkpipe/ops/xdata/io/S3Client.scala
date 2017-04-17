/**
 * Copyright © 2013-2017 Uncharted Software Inc.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 *
 * http://uncharted.software/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package software.uncharted.sparkpipe.ops.xdata.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{CannedAccessControlList, ObjectMetadata, PutObjectRequest}
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

  def upload(data: Seq[Byte], bucketName: String, key: String, contentType: String = "application/octet-stream", compress: Boolean = true): Boolean = {
    try {
      val streamData = if(compress) zipData(data.toArray).get else data.toArray
      val is = new ByteArrayInputStream(streamData)
      val meta = new ObjectMetadata()
      meta.setContentLength(streamData.length)

      meta.setContentType(contentType)
      if (compress) { meta.setContentEncoding("gzip") }

      s3Client.putObject(new PutObjectRequest(bucketName, key, is, meta)
        .withCannedAcl(CannedAccessControlList.Private))
      true
    } catch {
      case e: Exception => error(s"Failed to upload $key", e); false
    }
  }

  def download(bucketName: String, key: String, compress: Boolean = true): Option[Seq[Byte]]= {
    try {
      val dis = s3Client.getObject(bucketName, key).getObjectContent
      if (compress) {
        Some(unzipData(dis).get.toSeq)
      } else
      {
        val bos = new ByteArrayOutputStream()
          try {
            val bufferSize = 8192
            val buffer = Array.ofDim[Byte](bufferSize)

            var bytesRead =  dis.read(buffer, 0, bufferSize)
            while (bytesRead != -1) {
              bos.write(buffer, 0, bytesRead)
              bytesRead = dis.read(buffer)
            }
          } catch {
            case e: Exception => error("Failed to decompress file", e)
          } finally {
            bos.close()
          }
        Some(bos.toByteArray())
      }
    } catch {
      case e: Exception => error(s"Failed to download $key", e); None
    }
  }

  def zipData (dataToBeCompressed: Array[Byte]): Option[Array[Byte]] = {
    //create output stream to write data in and the filter that would compress the data going in
    val bos = new ByteArrayOutputStream(dataToBeCompressed.length)
    try {
      val GzipFilter = new GZIPOutputStream(bos)
      //write to outputstream using the filter
      GzipFilter.write(dataToBeCompressed, 0, dataToBeCompressed.length)
      GzipFilter.flush()
      GzipFilter.close()
      Some(bos.toByteArray())
    } catch {
      case e: Exception => error("Failed to compress file", e); None
    } finally {
      bos.close()
    }
  }

  def unzipData (compressedInputStream: InputStream): Option[Array[Byte]] = {
    //create a ByteArrayOutputStream object to write the decompressed data into; convert this to an array of bytes after.
    val bos = new ByteArrayOutputStream()
      try {
        //add the input stream into the filter GZIP input stream
        val GUnzipFilter = new GZIPInputStream(compressedInputStream)

        val bufferSize = 8192
        val buffer = Array.ofDim[Byte](bufferSize)

        var bytesRead =  GUnzipFilter.read(buffer, 0, bufferSize)
        while (bytesRead != -1) {
          bos.write(buffer, 0, bytesRead)
          bytesRead = GUnzipFilter.read(buffer)
        }
        GUnzipFilter.close()
        Some(bos.toByteArray)
      } catch {
        case e: Exception => error("Failed to decompress file"); None
      } finally {
        bos.close()
      }

  }

}
