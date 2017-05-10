package au.org.ala.biocache.spark

import org.apache.spark.{SparkConf, SparkContext}
import com.lucidworks.spark.rdd.SolrRDD
import org.apache.spark
import org.apache.spark.sql.SQLContext

/**
  * This is a test to evaluate the feasibility of exporting from SOLR using Spark.
  */
object SOLRExport {
  val fields = List("family", "basis_of_record")


    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Spark SOLR example")
      conf.setIfMissing("spark.master", "local[2]")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)

      try {

        val occurrences = sqlContext.read.format("solr").options(
          Map("zkHost" -> "35.157.64.9", "collection" -> "biocache")
        ).load
          .filter("data_resource_uid='dr346'")

        occurrences.printSchema()
        occurrences.show()
        occurrences.registerTempTable("occurrence")
        sqlContext.sql("SELECT family,count(*) AS cnt FROM occurrence GROUP BY family ORDER BY cnt DESC").show()

        // The following is a workaround to circumvent the fact that the CSV writers in Spark 1.6 are not partitionable.
        // We save as a parquet file per family, and then read each parquet file and safe it as a CSV.
        occurrences.write.partitionBy("family").format("parquet").save("/tmp/export/transient/")
        sqlContext.sql("SELECT distinct family FROM occurrence").collect().map(family => {
          val familyName = family.getString(0)
          println("Opening " + familyName)
          val df = sqlContext.read.format("parquet").load("/tmp/export/transient/family=" + familyName)
          df.write.format("com.databricks.spark.csv").save("/tmp/export/" + familyName)
        })

      } finally {
        sc.stop()
      }
    }
}
