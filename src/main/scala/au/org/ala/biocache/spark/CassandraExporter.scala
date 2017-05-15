package au.org.ala.biocache.spark

import java.io.File
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra._

/**
  * This is a test to evaluate the feasibility of exporting from Cassandra using Spark.
  */
object CassandraExporter {

  val fields = List("dataresourceuid", "uuid", "catalogNumber", "collectionCode", "institutionCode", "scientificName_p", "recordedBy",
    "taxonConceptID_p",
    "taxonRank_p", "kingdom_p", "phylum_p", "classs_p", "order_p", "family_p", "genus_p", "species_p"
    ,
    "decimalLatitude_p", "decimalLongitude_p", "coordinatePrecision", "coordinateUncertaintyInMeters_p",
    "maximumElevationInMeters", "minimumElevationInMeters",
    "minimumDepthInMeters", "maximumDepthInMeters", "continent", "country", "stateProvince", "county", "locality",
    "year_p", "month_p", "day_p", "basisOfRecord_p", "identifiedBy", "occurrenceRemarks", "locationRemarks",
    "recordNumber", "vernacularName_p", "individualCount", "eventID", "geodeticDatum_p",
    "eventDate_p"
  )

  def main(args:Array[String]) : Unit = {

    val outputDirPath = "/data/biocache-exports/"
    val outputDir = new File(outputDirPath)
    if(outputDir.exists()) {
      outputDir.delete()
    }

    val conf = new SparkConf().setAppName("Cassandra Bulk Export")
    conf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(conf)

    try {
      val sqlContext = new SQLContext(sc)
      val df = sqlContext.read.cassandraFormat("occ", "occ").load()
      df.registerTempTable("occ")
      sqlContext.udf.register("clean", (input: String) =>
        if(input != null){
          input.replaceAll("[\\t\\n\\r]", " ")
        } else {
          ""
        }
      )

      val exportQuery = "SELECT " + fields.map(field => "clean(" + field.toLowerCase + ") AS " + field.toLowerCase).mkString(",") + " FROM occ"

//       Write a single parquet file of cleaned data per data resource
      sqlContext.sql(exportQuery).write.partitionBy("dataresourceuid").format("parquet").save("/data/biocache-exports/transient/")


//      sqlContext.sql("SELECT dataresourceuid, uuid FROM occ")
//      .write
//        .partitionBy("dataresourceuid")
//        .format("parquet")
//        .save("/data/biocache-exports/transient/")

      // The following is a workaround to circumvent the fact that the CSV writers in Spark 1.6 are not partitionable.

      // get the dataresource keys
      val resourceUids = sqlContext.sql("SELECT distinct dataresourceuid FROM occ").collect()

      // for each resource, read the parquet file and write a CSV
      resourceUids.map(dataresourceuid => {
        val drUid = dataresourceuid.getString(0)
        val df = sqlContext.read.format("parquet").load("/data/biocache-exports/transient/dataresourceuid=" + drUid)
        df.write.format("com.databricks.spark.csv").save("/data/biocache-exports/" + drUid)
      })


//      val exportQuery = "SELECT " + fields.map(field => "first(clean(" + field.toLowerCase + ")) AS " + field.toLowerCase ).mkString(",") + " FROM occ"
//
//      // Write a single parquet file of cleaned data per data resource
//      sqlContext.sql(exportQuery).write.partitionBy("dataresourceuid").format("parquet").save("/data/biocache-exports/transient/")
//
//      // The following is a workaround to circumvent the fact that the CSV writers in Spark 1.6 are not partitionable.
//
//      // get the dataresource keys
//      val resourceUids = sqlContext.sql("SELECT distinct dataresourceuid FROM occ").collect()
//
//      // for each resource, read the parquet file and write a CSV
//      resourceUids.map(dataresourceuid => {
//        val drUid = dataresourceuid.getString(0)
//        val df = sqlContext.read.format("parquet").load("/data/biocache-exports/transient/dataresourceuid=" + drUid)
//        df.write.format("com.databricks.spark.csv").save("/data/biocache-exports/" + drUid)
//      })

//      val exportQuery = "SELECT first(dataresourceuid), " + fields.map("first(clean(" + _.toLowerCase + "))").mkString(",") +
//        " FROM occ"
//
//      val exportQuery = "SELECT " + fields.map("first(clean(" + _.toLowerCase + "))  " + _.toLowerCase).mkString(",") + " FROM occ"
//
//
//      // The following is a workaround to circumvent the fact that the CSV writers in Spark 1.6 are not partitionable.
//      // We save as a parquet file per family, and then read each parquet file and safe it as a CSV.
//      sqlContext.sql(exportQuery).write.partitionBy("dataresourceuid").format("parquet").save("/data/biocache-exports/transient/")
//      sqlContext.sql("SELECT distinct dataresourceuid FROM occ").collect().map(dataresourceuid => {
//        val drUid = dataresourceuid.getString(0)
//        val df = sqlContext.read.format("parquet").load("/data/biocache-exports/transient/dataresourceuid=" + drUid)
//        df.write.format("com.databricks.spark.csv").save("/data/biocache-exports/" + drUid)
//      })

    } finally {
      sc.stop()
    }

  }
}
