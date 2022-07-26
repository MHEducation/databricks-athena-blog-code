package com.mhedu.delta

import com.mhedu.athena.AthenaQueryClient
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.Column

class DeltaIntegration(athenaDatabase: String, athenaWorkGroup: String) {

  val athenaClient = new AthenaQueryClient(athenaDatabase, athenaWorkGroup)

  /**
   * Get the DBFS path for the specified Delta table
   */
  private def getTableDBFSPath(schema: String, tableName: String): String = {
    spark.sql(s"describe table extended $schema.$tableName")
      .filter(col("col_name") === "Location").take(1)(0).get(1).toString
  }

  /**
   * Set the auto-update manifest property to TRUE
   */
  private def setAutoUpdateManifest(schema: String, tableName: String, s3Path: String): Unit = {
    val sql = s"ALTER TABLE delta.`$s3Path` SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)"

    println(s"Running: $sql\n")

    spark.sql(sql)
    ()
  }

  def isPartitionColumn(columnName: Column) = {
    columnName.startsWith("partition_") && (columnName.notEqual("partition_key"))
  }

  def getDataTypeFromColumnName(columnName: String, schema: String, tableName: String): String = {
    return spark.sql(s"describe table $schema.$tableName").filter(col("col_name").contains(columnName)).select("data_type").collect().toList.map(f=>f.getString(0)).mkString("")
  }

  import org.apache.spark.sql.SparkSession

  private val spark = SparkSession
    .builder()
    .appName("Athena-Delta")
    .getOrCreate()

  /*
   * Generate the PARTITIONED BY clause of the create table statement
   */
  def getPartitionedByClause(schema: String, tableName: String) = {
    val partitionColumns = spark.sql(s"describe table extended $schema.$tableName").filter(col("col_name").startsWith("Part ")).select("data_type").collect().toList.map(f => f.getString(0))
    val columnSpecs = (for (i <- partitionColumns; (columnName, dataType) = (i.toString(),getDataTypeFromColumnName(i.toString(),schema,tableName)))
      yield s"$columnName $dataType").mkString(", ")

    if (partitionColumns.length > 0) {
      s"PARTITIONED BY ($columnSpecs)"
    }
    else {
      ""
    }
  }

  /**
   * Add quotes to keywords
   */
  private def quoteKeywords(columnName: String) = {
    columnName match {
      case "date" => "`date`"
      case _ =>
        if (columnName.contains("$")) {
          s"`$columnName`"
        }
        else {
          columnName
        }
    }
  }

  /**
   * Generate the Athena create table statement
   *
   * @param schema
   * @param tableName
   * @param s3Path
   *
   * @return String Create table statement
   */
  private def generateDDL(schema: String, tableName: String, s3Path: String) = {
    val partitionColumns = spark.sql(s"describe table extended $schema.$tableName").filter(col("col_name").startsWith("Part ")).select("data_type").collect().toList.map(f => f.getString(0))
    val columnsAndTypes = spark.sql(s"describe table $schema.$tableName").filter((!col("col_name").contains("partition"))
      &&
      (!col("col_name").contains("Part")) &&
      (!col("col_name").isin(partitionColumns:_*)) &&
      length(col("col_name")) > 0
    )
    val x = columnsAndTypes.collect()
    val columnSpecs = (for (i <- x; (columnName, dataType) = (i(0).toString, i(1)); escapedColumnName = quoteKeywords(columnName))
      yield s"$escapedColumnName $dataType").mkString(", ")

    val formats =
      """
      ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
      STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
      OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    """
    val partitionedByClause = getPartitionedByClause(schema, tableName)

    println(s"partitionedBy: $partitionedByClause")

    s"""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName ($columnSpecs)

     $partitionedByClause
     $formats
     LOCATION '$s3Path/_symlink_format_manifest'"""
  }

  /**
   * Get S3 Path for Delta table
   *
   * @param schema
   * @param tableName
   *
   * @return String S3 Path
   */
  def getS3Path(schema: String, tableName: String): String = {
    val dbfsPath = getTableDBFSPath(schema, tableName)
    val dbfsTableVersionName = dbfsPath.substring(dbfsPath.lastIndexOf('.') + 1)
    val bucket = if (schema.equals("prod")) {
      "prod-bucket-name"
    }
    else {
      "non-prod-bucket-name"
    }
    val schemaPath = schema.replace("_", "/")

    s"s3://$bucket/delta-tables/$schemaPath/$schema.$dbfsTableVersionName"
  }

  /**
   * Generate the create table statement for Athena
   *
   * @param schema Delta table schema
   * @param tableName Delta table name
   *
   * @return String Create table statement
   */
  def generateAthenaCreateTable(schema: String, tableName: String) = {
    val s3Path = getS3Path(schema, tableName)

    generateDDL(schema, tableName, s3Path)
  }

  /**
   * Generate the manifiest for the Delta table and set auto-update to true.
   *
   * @return String The create table statement for Athena
   */
  def generateAthenaManifest(schema: String, tableName: String): Unit = {
    val dbfsPath = getTableDBFSPath(schema, tableName)
    val deltaTable = DeltaTable.forPath(dbfsPath)
    val dbfsTableVersionName = dbfsPath.substring(dbfsPath.lastIndexOf('.') + 1)
    val s3Path = getS3Path(schema, tableName)

    println(s"DBFS path for table is: $dbfsPath")
    println(s"DBFS table versioned name: $dbfsTableVersionName\n")
    println(s"s3Path: $s3Path\n")

    deltaTable.generate("symlink_format_manifest")
    setAutoUpdateManifest(schema, dbfsTableVersionName, s3Path)
  }

  /**
   * Run MSCK REPAIR TABLE on an Athena table.
   *
   * @param Athena          table
   * @param athenaDatabase  Target Athena database
   * @param athenaWorkgroup Athena execution workgroup
   */
  def msckRepairTable(athenaTable: String, athenaDatabase: String, athenaWorkgroup: String) = {
    athenaClient.submitQuery(s"msck repair table $athenaTable")
  }

  /**
   * Create an Athena table based on existing Delta table
   *
   * @param deltaTable      Source Delta table
   * @param repairTable     True indicates to run 'msck repair table'
   * @param validate        True indicates to run validation query
   */
  def createAthenaTable(deltaTable: String, repairTable: Boolean = true, validate: Boolean = true) = {
    val index = deltaTable.indexOf('.')

    if (index == -1) {
      System.err.println("No dot separator found in Delta table name: schema.tableName")
      false
    }
    else {
      val schema = deltaTable.substring(0, index)
      val tableName = deltaTable.substring(index + 1)
      val s3Path = getS3Path(schema, tableName)
      val createTableStmt = generateDDL(schema, tableName, s3Path)

      generateAthenaManifest(schema, tableName)

      println(s"Submitting Athena create table statement: $createTableStmt\n")
      athenaClient.submitQuery(createTableStmt)

      if (repairTable) {
        System.out.println(s"Running: msck repair table $tableName")
        val repairQueryId = msckRepairTable(tableName, athenaDatabase, athenaWorkGroup)

        if (validate) {
          athenaClient.waitForQueryToComplete(repairQueryId)
          System.out.println(s"\nCompleted: msck repair table $tableName")
        }
      }

      if (validate) {
        System.out.println(s"\nRunning test query: select * from $tableName limit 1")
        val testQueryId = athenaClient.submitQuery(s"select * from $tableName limit 1")
        athenaClient.waitForQueryToComplete(testQueryId)

        if (athenaClient.checkQueryResult(testQueryId)) {
          System.out.println(s"\nSuccessfully validated table $tableName\n")
          true
        }
        else {
          System.out.println(s"\nFailed validation of table $tableName.  No rows returned on test query\n")
          false
        }
      }
      else {
        true
      }
    }
  }

  /**
   * This method will create in Athena tables from a source Delta database.
   *
   * @param deltaDatabase Source Delta database
   * @param athenaDatabase Target Athena database
   * @param athenaWorkGroup Athena execution workgroup.
   * @param pattern Pattern to use for matching source Delta table names
   *
   * @return List[Bool] List indicating the success fail result status for each table
   */
  def deployDeltaDatabaseToAthena(deltaDatabase: String, pattern: String = "*"): List[Boolean] = {
    val tablesDF = spark.sql(s"SHOW TABLES FROM $deltaDatabase LIKE '$pattern'")

    import spark.implicits._  // Required for next line
    val deltaTables = tablesDF.select(col ="tableName").map(f => f.getString(0)).collect.toList

    deltaTables map { deltaTable =>
      println(s"Processing table $deltaTable")
      createAthenaTable(s"$deltaDatabase.$deltaTable")
    }
  }

}
