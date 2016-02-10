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
<<<<<<< Updated upstream
//The table will have one family column: TileData (name not finalized)
  //no qualifiers, that adds complexity to rowID
//So we need functions to create these tables for each layer.
//We also need to add the data into the layer table
  //every file is a row, and adds to the TileData familyCol
object HBaseConnector {
  //S3Client's object that creates a new instance of S3Client doesn't have an
  //= between the function definition and parameters. Why?
  def apply(zookeeperQuorum: String, zookeeperPort: String, hBaseMaster: String) : HBaseConnector = {
    new HBaseConnector(zookeeperQuorum, zookeeperPort, hBaseMaster)
  }
}

class HBaseConnector(zookeeperQuorum, zookeeperPort, hBaseMaster) extends Logging {
  private val connection = createConnection(zookeeperQuorum, zookeeperPort, hBaseMaster)
  private val admin = connection.getAdmin()

=======
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
>>>>>>> Stashed changes
  //should this be a private method?
  //this method checks if a table exists and creates one if it doesn't
      //is this okay to do? or should I do
  def createTable(tableName: String, colName: String): Boolean = {
    try {
      if (!admin.tableExists(TableName.valueOf(tableName)))
      {
        val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
        tableDescriptor.addFamily(new HColumnDescriptor(colName))
        admin.createTable(tableDescriptor)
<<<<<<< Updated upstream
      }
      true
=======
        true
      }
      else { error(s"$tableName already exists"); false }
>>>>>>> Stashed changes
    } catch {
      case e: Exception => error(s"Failed to create table $tableName"); false
    }
  }

<<<<<<< Updated upstream

//ONE WAY TO CREATE THE TABLES AND STUFF CORRECTLY BEFORE WORKING WITH THE CLIENT:
  //HAVE AN OVERLOAD METHOD THAT OBTAINS THE TABLENAME AND COLUMN FAMILIES AND CREATES THE TABLES.
  //THIS WAY YOU JUST INITIALIZE IT ONCE
    //ONLY ISSUE WITH THIS IS IF YOU WANT TO USE THE CONNECTOR TO PLAY WITH EXISTING TABLES
      //THEREFORE THERE MUST BE A CHECK FOR THE OTHER METHODS TO SEE IF TABLE EXISTS
        //OR..AN ERROR EXCEPTION IF TABLE DOESN'T EXIST.
            //THIS IS THE BETTER OPTION.
//BOTH WRITE METHODS ASSUME YOU WANT TABLENAME IS CORRECT. IS THIS A WRONG ASSUMPTION?
  //BECAUSE IF TABLENAME DOESN'T EXIST, THE CURRENT SETUP IS THAT A TABLE IS CREATED
    //ASSUMING THAT THE TABLENAME PASSED IN IS ENTERED CORRECTLY AND NOT A MISTAKE

//CURRENTLY THESE WRITES WRITE TO ONE COLUMN.
=======
//CURRENTLY THESE WRITES ONLY WRITE TO ONE COLUMN.
>>>>>>> Stashed changes
//IMPROVEMENTS:
  //CHECK COLNAME IS AN ACTUAL COLUMN FAMILY MEMBER BEFORE ADDING COLUMN FAMILY TO PUT INFO.
    //IF NOT CREATE THE COLUMN FAMILY FOR THAT TABLE AND ADD MORE INFO RELATED TO THAT TILE DATA.

<<<<<<< Updated upstream

  //so right now writeRow Allows you to add a row with one column name.
  //should be another way of creating a row with a map for a value of each column name
  def writeRow(tableName: String, colName: String, rowID: String, data: Seq[Byte]): Boolean = {
    try {
      val table = this.getOrCreateTable(tableName, colName)
      val putInfo = new Put(rowID.getBytes())
      putInfo.addColumn(colName.getBytes(), new byte[0], data.toArray())
      table.put(putInfo)
      table.close()
      true
=======
  def writeRow(tableName: String, colName: String, rowID: String, data: Seq[Byte]): Boolean = {
    try {
      val rowDataToList = List((rowID,data))
      this.writeRows(tableName, colName, rowDataToList)
>>>>>>> Stashed changes
    } catch {
      case e: Exception => error(s"Failed to write row into table $tableName"); false
    }
  }

  //batch row insert method obtains a list of row info
<<<<<<< Updated upstream
  //row info is stored in the format: data, rowId
  //LIST VS SEQ, WHATS BETTER?
  def writeRows(tableName: String, colName: String, listOfRowInfo: List[(Seq[Byte], String)]): Boolean = {
    try {
      val table = this.getOrCreateTable(TableName.valueOf(tableName))
      val putList = listOfRowInfo.map {rowInfo =>
        new Put(rowInfo._2.getBytes()).addColumn(colName.getBytes(), new byte[0], rowInfo._1.toArray())
      }
      table.put(putList)
      table.close()
      true
    } catch {
      case e: Exception => error(s"Failed to write rows into table $tableName"); false
=======
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
>>>>>>> Stashed changes
    }
  }

  //attempts to gets Table. Returns nothing if table doesn't exist
<<<<<<< Updated upstream
  //THIS METHOD IS JUST TO GET THE USER OF THE CONNECTION TO SPECIFICALLY
    //CREATE A TABLE THEMSELVES IF HBASE CANNOT GET A TABLE.
=======
>>>>>>> Stashed changes
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

<<<<<<< Updated upstream
  //gets Table, and if table isn't there; creates table.
  private def getOrCreateTable(tableName: String, colName: String): Option[Table] {
    try {
      if(admin.tableExists(TableName.valueOf(tableName))) {
        Some(connection.getTable(TableName.valueOf(tableName))
      }
      else {
        this.createTable(tableName, colName)
        Some(connection.getTable(TableName.valueOf(tableName)))
      }
    } catch {
      case e: Exception => error(s"could not get table: $tableName"); None
    }
  }

=======
>>>>>>> Stashed changes
}
