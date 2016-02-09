package software.uncharted.xdata.ops.io
import org.scalatest.{BeforeAndAfterAll, FunSpec, Tag}
import java.io.ByteArrayInputStream

object HBaseTest extends Tag("hBase.test")

class HBaseConnectorTest extends FunSpec with BeforeAndAfterAll {
  private lazy val s3 = HBaseConnector(sys.env("AWS_ACCESS_KEY"), sys.env("AWS_SECRET_KEY"))
  createTable

  writeRow

  writeRows

  getTable

  createConnection

  getOrCreateTable

  close
}
