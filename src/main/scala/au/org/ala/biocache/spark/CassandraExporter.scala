package au.org.ala.biocache.spark

import java.io.File
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra._
/**
  * Created by mar759 on 3/05/2017.
  */
object CassandraExporter {

  val fields = List("uuid", "catalogNumber", "collectionCode", "institutionCode", "scientificName_p", "recordedBy",
    "taxonConceptID_p",
    "taxonRank", "kingdom_p", "phylum_p", "classs_p", "order_p", "family_p", "genus_p", "species_p",
    "decimalLatitude_p", "decimalLongitude_p", "coordinatePrecision", "coordinateUncertaintyInMeters",
    "maximumElevationInMeters", "minimumElevationInMeters",
    "minimumDepthInMeters", "maximumDepthInMeters", "continent", "country", "stateProvince", "county", "locality",
    "year_p", "month_p", "day_p", "basisOfRecord", "identifiedBy", "occurrenceRemarks", "locationRemarks",
    "recordNumber", "vernacularName", "individualCount", "eventID", "geodeticDatum",
     "eventDate_p"
  )

  def main(args:Array[String]) : Unit = {

    if(args.length != 1){
      println("Please supply a data resource UID")
      System.exit(1)
    }

    val outputDirPath = "/data/biocache-exports/" + args(0)
    val outputDir = new File(outputDirPath)
    if(outputDir.exists()) {
      outputDir.delete()
    }

    val conf = new SparkConf().setAppName("Cassandra Bulk Export")
    conf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(conf)
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

    val sql = "SELECT " + fields.map("clean(" + _.toLowerCase + ")").mkString(",") + " FROM occ where dataresourceuid = '" + args(0) + "'"
    println(sql)
    val df2 = sqlContext.sql(sql)
    df2.write.format("com.databricks.spark.csv").save(outputDirPath)
  }
}
