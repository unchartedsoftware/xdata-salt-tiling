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
import org.apache.hadoop.hbase.client.*;

//initial HBaseConnector is to write layer details into HBase like S3 does.
//Each Layer has its own table
//So each tableName -> layerName
//Each Layer table will have the file name as the key (Like S3)
  //in this case the key will be the rowID
//The table will have one family column

  //CURRENTLY CLASS IS SET UP SUCH THAT THE CLIENT CAN MAKE THEIR OWN COLUMN FAMILY NAME WHEN CREATING TABLES
    //DO WE WANT IT LIKE THIS OR DO WE WANT THE FAMILY COLUMN NAME TO BE A CONSTANT FOR EACH LAYER TABLE


object HBaseConnector {
  def apply(zookeeperQuorum: String, zookeeperPort: String, hBaseMaster: String) : HBaseConnector = {
    new HBaseConnector(zookeeperQuorum, zookeeperPort, hBaseMaster, None, None)
  }

  def apply(zookeeperQuorum: String, zookeeperPort: String, hBaseMaster: String, tableName: String, colName: String) : HBaseConnector = {
    new HBaseConnector(zookeeperQuorum, zookeeperPort, hBaseMaster Some(tableName), Some(colName))
  }
}

class HBaseConnector(zookeeperQuorum, zookeeperPort, hBaseMaster, initTableName, initColName) extends Logging {
  private val connection = createConnection(zookeeperQuorum, zookeeperPort, hBaseMaster)
  private val admin = connection.getAdmin()
  initTableIfNeeded(initTableName, initColName)
  //should this be a private method?
  def createTable(tableName: String, colName: String): Boolean = {
    try {
      if (!admin.tableExists(TableName.valueOf(tableName)))
      {
        val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
        tableDescriptor.addFamily(new HColumnDescriptor(colName))
        admin.createTable(tableDescriptor)
        true
      }
      else { error(s"$tableName already exists"); false }
    } catch {
      case e: Exception => error(s"Failed to create table $tableName"); false
    }
  }

  private def initTableIfNeeded(tableName: Option[String], colName: Option[String]): Unit {
    val tableItems = List(tableName, colName).flatMap(item => item)
    if (getTable() == None && !strings.isEmpty) {
      createTable(tableItems(0), tableItems(1)
    }
  }

//CURRENTLY THESE WRITES ONLY WRITE TO ONE COLUMN.
//IMPROVEMENTS:
  //CHECK COLNAME IS AN ACTUAL COLUMN FAMILY MEMBER BEFORE ADDING COLUMN FAMILY TO PUT INFO.
    //IF NOT CREATE THE COLUMN FAMILY FOR THAT TABLE AND ADD MORE INFO RELATED TO THAT TILE DATA.

  def writeRow(tableName: String, colName: String, rowID: String, data: Seq[Byte]): Boolean = {
    try {
      val rowDataToList = List((rowID,data))
      this.writeRows(tableName, colName, rowDataToList)
    } catch {
      case e: Exception => error(s"Failed to write row into table $tableName"); false
    }
  }

  //batch row insert method obtains a list of row info
  //row info is stored in tuple format: (rowId, data)
  def writeRows(tableName: String, colName: String, listOfRowInfo: List[(String, Seq[Byte])]) {
    try {
      val table = this.getTable(tableName)
      val putList = listOfRowInfo.map {rowInfo =>
        new Put(rowInfo._1.getBytes()).addColumn(colName.getBytes(), new byte[0], rowInfo._2.toArray())
      }
      table.flatMap(_.put(putList))
      val checkEmpty = table.flatMap(_.close())
      if (checkEmpty.isEmpty) false else true
      true
    } catch {
      case e: Exception => error(s"Failed to write into table $tableName"); false
    }
  }

  //attempts to gets Table. Returns nothing if table doesn't exist
  def getTable(tableName: String): Option[Table] {
    try {
      if(admin.tableExists(TableName.valueOf(tableName))) {
        Some(connection.getTable(TableName.valueOf(tableName))
      }
      else {
        //OR SHOULD I THROW AN EXCEPTION SO THAT IT CATCHES BELOW?
        None
      }
    } catch {
      case e: Exception => error(s"table doesn't exist $tableName"); None
    }
  }

  def close(): Unit = {
    admin.close()
  }

  //if return value type gives an ambiguity error somehow, try HBaseAdmin
  private def createConnection(zookeeperQuorum: String, zookeeperPort: String, hBaseMaster: String): Admin = {
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", zookeeperQuorum)
    config.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    config.set("hbase.master", hBaseMaster)
    config.set("hbase.client.keyvalue.maxsize", "0")

    ConnectionFactory.createConnection(config)
  }

}
