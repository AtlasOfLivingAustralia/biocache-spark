package au.org.ala.biocache.spark

import java.io.File
import java.net.URL

import au.org.ala.biocache.spark.Configurations.CassandraExportConfiguration
import com.google.common.io.Resources
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra._
import org.apache.spark.storage.StorageLevel

import scala.sys.process._

/**
  * Performs an export of interpreted data from Cassandra into a DwC-A per data resource.
  *
  * Notes:
  * <ul>
  *   <li>This was written for the UK NBN use case, and only run in a single JVM</li>
  *   <li>This is expecting to use Spark 1.6.x initially to align with other Spark developments</li>
  *   <li>CSV output formats in 1.6 don't support partitionBy(...) - thus we save as Parquet and rewrite to CSV</li>
  * </ul>
  */
object CassandraExporter {
  val usage = "Usage: CassandraExporter configFile"
  val pathTransient = "/transient"
  val pathExport = "/full-export"

  // export definition which cleans data and maps to DwC terms
  val exportSQL =
    "SELECT " +
    "  dataresourceuid AS dataResourceUid, " +
    "  clean(uuid) AS occurrenceID, " +
    "  clean(catalognumber) AS catalogNumber, " +
    "  clean(collectioncode) AS collectionCode, " +
    "  clean(institutioncode) AS institutionCode, " +
    "  clean(scientificname_p) AS scientificName, " +
    "  clean(recordedby) AS recordedBy, " +
    "  clean(taxonconceptid_p) AS taxonConceptID, " +
    "  clean(taxonrank_p) AS taxonRank, " +
    "  clean(kingdom_p) AS kingdom, " +
    "  clean(phylum_p) AS phylum, " +
    "  clean(classs_p) AS classs, " + // reserved name in JVM
    "  clean(order_p) AS orderr, " + // reserved name in SQL
    "  clean(family_p) AS family, " +
    "  clean(genus_p) AS genus, " +
    "  clean(decimallatitude_p) AS decimalLatitude, " +
    "  clean(decimallongitude_p) AS decimalLongitude, " +
    "  clean(coordinateuncertaintyinmeters_p) AS coordinateUncertaintyInMeters, " +
    "  clean(maximumelevationinmeters) AS maximumElevationInMeters, " +
    "  clean(minimumelevationinmeters) AS minimumElevationInMeters, " +
    "  clean(minimumdepthinmeters) AS minimumDepthInMeters, " +
    "  clean(maximumdepthinmeters) AS maximumDepthInMeters, " +
    "  clean(continent) AS continent, " +
    "  clean(country_p) AS country, " +
    "  clean(stateprovince_p) AS stateProvince, " +
    "  clean(locality) AS locality, " +
    "  clean(year_p) AS year, " +
    "  clean(month_p) AS month, " +
    "  clean(day_p) AS day, " +
    "  clean(basisofrecord_p) AS basisOfRecord, " +
    "  clean(identifiedby) AS identifiedBy, " +
    "  clean(occurrenceremarks) AS occurrenceRemarks, " +
    "  clean(locationremarks) AS locationRemarks, " +
    "  clean(recordnumber) AS recordNumber, " +
    "  clean(vernacularname_p) AS vernacularName, " +
    "  clean(individualcount) AS individualCount, " +
    "  clean(eventid) AS eventID, " +
    "  clean(geodeticdatum_p) AS geodeticDatum, " +
    "  clean(eventdate_p) AS eventDate " +
    "FROM occurrence"

  def main(args:Array[String]) : Unit = {
    checkArgs(args) // sanitize input
    val config: CassandraExportConfiguration = Configurations.fromFile(args(0))
    init(config)

    val conf = new SparkConf().setAppName(config.appName)
    conf.setIfMissing("spark.master", "local[*]")
    conf.set("spark.cassandra.connection.host", config.cassandra.host)
    conf.set("spark.cassandra.connection.port", config.cassandra.port)

    // because we export only a narrow selection, the Cassandra connector will over estimate the number spark
    // partitions needed.  We agressively increase that from the default of 64 to compensate
    //conf.set("spark.cassandra.input.split.size_in_mb", "8192")
    // Removed because we pass this on the command line instead using --conf 'spark.cassandra.input.split.size_in_mb=8192'

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    try {
      val sqlContext = new SQLContext(sc)

      val source = sqlContext.read.cassandraFormat(config.cassandra.table, config.cassandra.keySpace).load()
      source.registerTempTable("occurrence")

      // register the UDF to cleans fields suitable for a CSV
      sqlContext.udf.register("clean", (input: String) =>
        if(input == null) "" else input.replaceAll("[\\t\\n\\r]", " ").trim
      )

      val df = sqlContext.sql(exportSQL)

      // NOTE: This is what was actually ran in the first version
      // Writes a single parquet file (default gzip compression) of cleaned data from Cassandra
      //df.write.format("parquet").save(config.outputDir + pathExport)

      // We ran the line above, and then continued again with this
      // If you have an export of cassandra already, you can use this instead
      //val df = sqlContext.read.format("parquet").load("/data/biocache-exports-SINGLE-GOOD/transient")

      val resourceUids = df.select(df("dataResourceUid")).distinct.collect()

      for (id <- resourceUids) {
        val drUid = id(0) // extract the actual resource id

        var filter = "dataResourceUid = '" + drUid + "'"
        println("Starting " + filter)
        val targetDir = config.outputDir + "/" + drUid

        // filter the source to the DR of interest, remove unused columns and save
        df.filter(filter).drop("dataResourceUid").write.format("com.databricks.spark.csv").save(targetDir)

        // working directory
        val dir = new File(targetDir)

        // Download the EML from the collectory WS
        new URL(config.collectoryUrl + "/ws/eml/" + drUid) #> new File(dir, "eml.xml") !!

        // Concatenate the CSV parts together
        val parts = dir.listFiles.filter(_.getName.startsWith("part")).toList
        val catCmd = "cat " + parts.map(_.getAbsolutePath).mkString(" ")
        catCmd #>> new File(dir, "occurrence.txt") !!

        Resources.getResource("meta.xml") #> new File(dir, "meta.xml") !!

        // cleanup by deleting unused files
        dir.listFiles().filter(file => {
          !(file.getName == "occurrence.txt" ||
          file.getName == "eml.xml" ||
          file.getName == "meta.xml")
        }).foreach(_.delete())

        // zip the targetDir into a DwC-A file
        val zipCmd = "zip -r -j " + targetDir + ".zip " + targetDir
        zipCmd !!

        // clean up the targetDir
        val rmCmd = "rm -fr " + targetDir
        rmCmd !!
      }

    } finally {
      sc.stop()
    }

  }

  /**
    * Clean up ready for an export run
    */
  def init(config: CassandraExportConfiguration) : Unit = {
    val outputDir = new File(config.outputDir)

    if (outputDir.exists()) {
      if (config.deleteOutputDir) {
        val rmCmd = "rm -fr " + config.outputDir
        rmCmd !!
      } else {
        println("OutputDir exists and config does not allow it to be deleted")
        System.exit(1)
      }
    }
  }

  /**
    * Sanitizes application arguments.
    */
  private def checkArgs(args: Array[String]) = {
    assert(args !=null && args.length==1, usage)
  }
}
