package au.org.ala.biocache.spark

import java.io.File
import java.net.URL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra._

import scala.sys.process._

/**
  * This is a test to evaluate the feasibility of exporting from Cassandra using Spark.
  */
object CassandraExporter {

  val fields = List("dataresourceuid", "uuid", "catalogNumber", "collectionCode", "institutionCode", "scientificName_p", "recordedBy",
    "taxonConceptID_p",
    "taxonRank_p", "kingdom_p", "phylum_p", "classs_p", "order_p", "family_p", "genus_p", "species_p",
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
    conf.set("spark.cassandra.connection.host", "127.0.0.1") // TODO
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

      // Write a single parquet file of cleaned data per data resource
      sqlContext.sql(exportQuery).write.partitionBy("dataresourceuid").format("parquet").save("/data/biocache-exports/transient/")


      // The following is a workaround to circumvent the fact that the CSV writers in Spark 1.6 are not partitionable.

      // get the dataresource keys
      val resourceUids = sqlContext.sql("SELECT distinct dataresourceuid FROM occ").collect()

      // for each resource, read the parquet file and write a CSV
      resourceUids.map(dataresourceuid => {
        val drUid = dataresourceuid.getString(0)
        val df = sqlContext.read.format("parquet").load("/data/biocache-exports/transient/dataresourceuid=" + drUid)
        df.write.format("com.databricks.spark.csv").save("/data/biocache-exports/" + drUid)

        // working directory
        val dir = new File("/data/biocache-exports/" + drUid);

        // Download the EML
        new URL("https://registry.nbnatlas.org/ws/eml/" + drUid) #> new File(dir, "eml.xml") !!


        // Concatenate the CSV parts together
        val parts = new File("/data/biocache-exports/" + drUid).listFiles.filter(_.getName.startsWith("part")).toList
        val catCmd = "cat " + parts.map(_.getAbsolutePath).mkString(" ")
        catCmd #>> new File(dir, "occurrence.txt") !!

        // TODO: Add a standard meta.xml

        // cleanup by deleting unused files
        dir.listFiles().filter(file => {
          !(file.getName == "occurrence.txt" ||
          file.getName == "eml.xml" ||
          file.getName == "meta.xml")
        }).foreach(_.delete())

        // zip them into DwC-A files
        val zipCmd = "zip -r /data/biocache-exports/" + drUid + ".dwca /data/biocache-exports/" + drUid
        zipCmd !!

        val rmCmd = "rm -fr /data/biocache-exports/" + drUid
        rmCmd !!

      })

    } finally {
      sc.stop()
    }

  }
}
