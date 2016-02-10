package software.uncharted.xdata.ops.io
import org.scalatest.{BeforeAndAfterAll, FunSpec, Tag}
import java.io.ByteArrayInputStream
import org.apache.hadoop.hbase.client.*;

object HBaseTest extends Tag("hBase.test")

class HBaseConnectorTest extends FunSpec with BeforeAndAfterAll {
  private lazy val s3 = HBaseConnector("uscc0-node08.uncharted.software", "2181", "hdfs://uscc0-master0.uncharted.software:60000")

  describe("HBaseConnector") {

    describe("#createTable") {
      it("should return true on table creation") {
        val result = s3.createTable("testTable", "testCol")
        assertResult(true)(result)
      }
      it("should return false when trying to create a table that already exists") {
        val result = s3.createTable("existingTable", "testCol")
        assertResult(false)(result)
      }
      it("should create a table in HBase when true is returned") {
        val result = s3.createTable(testTable2, testCol)
        val tableName = s3.getTable.getName().getNameAsString()
        assertResult(testTable2)(tableName)
      }
      // it("should not create a table in HBase when false is returned") {
      //
      // }
    }

    describe("#writeRow") {
      it("should return true on row insertion into table+column") {
        val result = s3.writeRow("testTable3", "testCol", "1", data)
        assertResult(true)(result)
      }

      it("should store the data in the specified table+column in HBase") {
        s3.writeRow("testTable3", "testCol", "2", data)
        //double check if this even works
        val rowData = s3.getTable("testTable3").get(new Get("2".getBytes()).addFamily("testCol".getBytes())).value()
        assertResult(data)(rowData)
      }

      it("should return false if column does not exist") {
        val result = s3.writeRow("testTable3", "nonExistantCol", "2", data)
        assertResult(false)(result)
      }

      it("should create the table specified if table does not exist") {
        s3.writeRow("testTable4", "testCol", "1", data)
        val table = s3.getTable("testTable4").getName().getNameAsString()
        assertResult("testTable4")(table)

      }

    }

    describe("#writeRows") {
      it("should return true when rows are written to table") {
        val result = s3.writeRows("testTable5", "testCol", List((rowID1, data), (rowID2, data)))
        assertResult(true)(result)
      }

      it("should return false when provided with a column that is not in the table") {
        val result = s3.writeRows("testTable5", "nonExistantCol", List((rowID1, data), (rowID2, data)))
        assertResult(false)(result)
      }

      it("should write the rows into HBase") {
        s3.writeRows("testTable5", "testCol",List((rowID3, data), (rowID4, data)))
        val table = s3.getTable("testTable5")
        List((rowID3, data), (rowID4, data)).foreach { row =>
          val rowData = table.get(new Get(row._1.getBytes()).addFamily("testCol".getBytes())).getValue("testCol".getBytes(), new Byte[0])
          assertResult(row._2)(rowData)
        }
      }
    }

    describe("#getTable") {
      it("should return a table instance") {
        val result = s3.getTable("testTable6")
        assertResult(dummyResult)(result)
      }

      it("should return none if table doesn't exist") {
        val result = s3.getTable("nonExistentTable")
        assertResult(None)(result)
      }
    }

    // describe("#createConnection") {
    //
    // }


    //do I even need a test case for this?
    // describe("#close") {
    //
    // }

  }

}
