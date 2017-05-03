package au.org.ala.biocache.spark
import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra._
/**
  * Created by mar759 on 3/05/2017.
  */
object CassandraExporter {

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("Cassandra Bulk Export").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.cassandraFormat("occ", "occ").load()
    df.registerTempTable("occ")

    val df2 = sqlContext.sql("SELECT uuid,dataresourceuid FROM occ")
    df2.write.format("com.databricks.spark.csv").save("/data/output")
  }

}
