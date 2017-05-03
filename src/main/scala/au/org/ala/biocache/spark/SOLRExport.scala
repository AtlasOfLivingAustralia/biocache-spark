package au.org.ala.biocache.spark

import org.apache.spark.{SparkConf, SparkContext}
import com.lucidworks.spark.rdd.SolrRDD
import org.apache.spark
import org.apache.spark.sql.SQLContext

/**
  * This is a test to evaluate the feasibility of exporting from SOLR using Spark.
  */
object SOLRExport {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Bulkload example")
      conf.setIfMissing("spark.master", "local[2]")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)

      try {

        val occurrences = sqlContext.read.format("solr").options(
          Map("zkHost" -> "35.157.64.9", "collection" -> "biocache")
        ).load
          .filter("data_resource_uid='dr819'")

        println(occurrences.count())



      } finally {
        sc.stop()
      }
    }
}
