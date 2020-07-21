package io.github.interestinglab.waterdrop.output.batch

import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import org.apache.http.HttpHost
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class ElasticsearchBinlog extends BaseOutput {

  var config: Config = ConfigFactory.empty()
  var pkNames: java.util.List[String] = _
  var bulkSize: Int = _
//  var isSetPkNames: Boolean = false

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("hosts")
    val nonExistsOptions: List[(String, Boolean)] = requiredOptions
      .map { optionName => (optionName, config.hasPath(optionName))
      }
      .filter { p =>
        val (optionName, exists) = p
        !exists
      }

    if (nonExistsOptions.isEmpty) {
      (true, "")
    } else {
      (
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "index_type" -> "bin-log",
        "bulk_size" -> 200
      )
    )
    pkNames = config.getStringList("pkNames")
    config = config.withFallback(defaultConfig)

    // if (config.hasPath("pkNames")) {
    //   isSetPkNames = true
    // }

    bulkSize = config.getInt("bulk_size")
  }

  override def process(df: Dataset[Row]) {

    val host = config.getString("hosts")
    val indexType = config.getString("index_type")

    df.foreachPartition { partitons => {
      val client = new RestHighLevelClient(
        RestClient.builder(
          new HttpHost(host.split(":")(0), host.split(":")(1).toInt, "http")
        )
      )
      val request = new BulkRequest()

      while (partitons.hasNext) {
        val row = partitons.next()

        val indexName = row.getAs[String]("table")
        val binlogType = row.getAs[String]("type")

        val dataObj = row.getValuesMap[Object](row.schema.fieldNames).asJava

        binlogType match {
          case "UPDATE" =>
            request.add(new UpdateRequest(indexName, primaryKey(row)).`type`(indexType).doc(dataObj))
          case "INSERT" =>
            request.add(new IndexRequest(indexName).id(primaryKey(row)).`type`(indexType).source(dataObj))
          case "DELETE" =>
            request.add(new DeleteRequest(indexName, primaryKey(row)).`type`(indexType))
          case _ =>
        }

        if (request.numberOfActions() > bulkSize) {
          val bulkResponse = client.bulk(request, RequestOptions.DEFAULT)

          if (bulkResponse.hasFailures) {
            log.warn(bulkResponse.buildFailureMessage())
          }
        }
      }
      client.close()
    }
    }
  }

  private def primaryKey(row: Row): String = {

    val key = new StringBuilder()
    val iter = pkNames.iterator()
    while (iter.hasNext) {
      val pk = iter.next()
      key.append(row.getAs[String](pk))
      if (iter.hasNext) {
        key.append("-")
      }
    }
    key.toString()
  }
}
