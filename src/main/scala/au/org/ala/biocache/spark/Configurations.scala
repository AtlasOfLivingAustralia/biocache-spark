package au.org.ala.biocache.spark

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.base.Charsets
import com.google.common.io.{Files, Resources}
import com.google.common.base.Preconditions._

/**
  * Utility builders
  */
object Configurations {

  /**
    * Returns the application cofiguration for the given file.
    *
    * @param file The YAML config file to read
    * @return The application configuration
    */
  def fromFile(filePath : String) : CassandraExportConfiguration = {
    //val confUrl = Resources.getResource(file)
    val mapper = new ObjectMapper(new YAMLFactory())
    val configAsString = Files.toString(new File(filePath), Charsets.UTF_8);
    val config: CassandraExportConfiguration = mapper.readValue(configAsString, classOf[CassandraExportConfiguration])
    config
  }

  /**
    * Configuration for the job to export Cassandra to DwC-A.
    */
  class CassandraExportConfiguration (
    @JsonProperty("appName") _appName: String,
    @JsonProperty("deleteOutputDir") _deleteOutputDir: Boolean,
    @JsonProperty("outputDir") _outputDir: String,
    @JsonProperty("collectoryUrl") _collectoryUrl: String,
    @JsonProperty("cassandra") _cassandra: CassandraConfiguration
  ) extends Serializable {
    val appName = checkNotNull(_appName, "appName cannot be null" : Object)
    val deleteOutputDir = checkNotNull(_deleteOutputDir, "deleteOutputDir cannot be null" : Object)
    val outputDir = checkNotNull(_outputDir, "outputDir cannot be null" : Object)
    val collectoryUrl = checkNotNull(_collectoryUrl, "collectoryUrl cannot be null" : Object)
    val cassandra = checkNotNull(_cassandra, "cassandra cannot be null" : Object)
  }

  class CassandraConfiguration (
    @JsonProperty("host") _host: String,
    @JsonProperty("port") _port: String,
    @JsonProperty("table") _table: String,
    @JsonProperty("keySpace") _keySpace: String
  ) extends Serializable {
    val host = checkNotNull(_host, "host cannot be null" : Object)
    val port = checkNotNull(_port, "port cannot be null" : Object)
    val table = checkNotNull(_table, "table cannot be null" : Object)
    val keySpace = checkNotNull(_keySpace, "keySpace cannot be null" : Object)
  }
}
