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
      val conf = new SparkConf().setAppName("Spark SOLR example")
      conf.setIfMissing("spark.master", "local[2]")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)

      try {

        val occurrences = sqlContext.read.format("solr").options(
          Map("zkHost" -> "35.157.64.9", "collection" -> "biocache")
        ).load
          .filter("data_resource_uid='dr819'")

        occurrences.printSchema()
        occurrences.show()
        occurrences.registerTempTable("occurrence")
        sqlContext.sql("SELECT family,count(*) AS cnt FROM occurrence GROUP BY family ORDER BY cnt DESC").show()


      } finally {
        sc.stop()
      }
    }
}
