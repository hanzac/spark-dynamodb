package org.apache.spark.dynamodb

import java.util.concurrent.atomic.AtomicLong

import com.amazonaws.services.dynamodbv2.document.{KeyConditions, RangeKeyCondition}
import com.amazonaws.services.dynamodbv2.document.spec.{QuerySpec, ScanSpec}
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity
import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Scan a DynamoDB table for use as a [[org.apache.spark.sql.DataFrame]].
  *
  * @param tableName        Name of the DynamoDB table to scan.
  * @param maybePageSize    DynamoDB request page size.
  * @param maybeSegments    Number of segments to scan the table with.
  * @param maybeRateLimit   Max number of read capacity units per second each scan segment
  *                         will consume from the DynamoDB table.
  * @param maybeRegion      AWS region of the table to scan.
  * @param maybeSchema      Schema of the DynamoDB table.
  * @param maybeCredentials By default, [[com.amazonaws.auth.DefaultAWSCredentialsProviderChain]]
  *                         will be used, which, which will work for most users. If you have a
  *                         custom credentials provider it can be provided here.
  * @param maybeEndpoint    Endpoint to connect to DynamoDB on. This is intended for tests.
  * @see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScanGuidelines.html
  */
private[dynamodb] case class DynamoDBRelation(
  tableName: String,
  maybeFilterExpression: Option[String],
  maybePageSize: Option[String],
  maybeSegments: Option[String],
  maybeRateLimit: Option[Int],
  maybeRegion: Option[String],
  maybeSchema: Option[StructType],
  maybeCredentials: Option[String] = None,
  maybeEndpoint: Option[String])
  (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedScan with PrunedFilteredScan with BaseScanner {

  private val log = LoggerFactory.getLogger(this.getClass)

  @transient private lazy val Table = getTable(
    tableName, maybeCredentials, maybeRegion, maybeEndpoint)

  private val pageSize = Integer.parseInt(maybePageSize.getOrElse("1000"))

  private val Segments = Integer.parseInt(maybeSegments.getOrElse("1"))

  // Infer schema with JSONRelation for simplicity. A future improvement would be
  // directly processing the scan result set.
  private val TableSchema = maybeSchema.getOrElse({
    val scanSpec = new ScanSpec().withMaxPageSize(1).withMaxResultSize(1)
    val result = Table.scan(scanSpec)
    val json = result.firstPage().iterator().map(_.toJSON)
    log.info("Complete result: " + result.firstPage.iterator.next.toJSON)
    val jsonSeq = json.toSeq
    val jsonRDD = sqlContext.sparkContext.parallelize(jsonSeq)
    val jsonDF = sqlContext.read.json(jsonRDD)
    jsonDF.schema
  })

  /** Get the relation schema.
    *
    * The user-defined schema will be used, if provided. Otherwise the schema is inferred
    * from one page of items scanned from the DynamoDB tableName.
    */
  override def schema: StructType = TableSchema

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val segments = 0 until Segments
    val scanConfigs = segments.map(idx => {
      ScanConfig(
        table = tableName,
        segment = idx,
        totalSegments = segments.length,
        pageSize = pageSize,
        maybeSchema = Some(schema),
        maybeRequiredColumns = Some(requiredColumns),
        maybeFilterExpression = maybeFilterExpression,
        maybeRateLimit = maybeRateLimit,
        maybeCredentials = maybeCredentials,
        maybeRegion = maybeRegion,
        maybeEndpoint = maybeEndpoint)
    })

    val tableDesc = Table.describe()

    log.info(s"Table ${tableDesc.getTableName} contains ${tableDesc.getItemCount} items " +
      s"using ${tableDesc.getTableSizeBytes} bytes.")

    log.info(s"Schema for tableName ${tableDesc.getTableName}: $schema")

    sqlContext.sparkContext
      .parallelize(scanConfigs, scanConfigs.length)
      .flatMap(scan)
  }

  def buildQuerySpec(spec: QuerySpec, filters: Array[Filter]): QuerySpec = {
    var keyCondExpr = ""
    var prevKeyCondExpr = ""
    val geAttributes = scala.collection.mutable.ArrayBuffer[String]()
    val leAttributes = scala.collection.mutable.ArrayBuffer[String]()
    for (f <- filters) {
      log.info(f.toString)
      prevKeyCondExpr = keyCondExpr

      if (keyCondExpr.length > 0)
        keyCondExpr += " and "

      val valueMap = if (f.isInstanceOf[EqualTo]) {
          keyCondExpr += f.asInstanceOf[EqualTo].attribute + " = :" + f.asInstanceOf[EqualTo].attribute
          Map(":" + f.asInstanceOf[EqualTo].attribute -> f.asInstanceOf[EqualTo].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[GreaterThan]) {
          keyCondExpr += f.asInstanceOf[GreaterThan].attribute + " > :" + f.asInstanceOf[GreaterThan].attribute
          Map(":" + f.asInstanceOf[GreaterThan].attribute -> f.asInstanceOf[GreaterThan].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[GreaterThanOrEqual]) {
          keyCondExpr = prevKeyCondExpr
          geAttributes += f.asInstanceOf[GreaterThanOrEqual].attribute
          Map(":" + f.asInstanceOf[GreaterThanOrEqual].attribute + "_ge" -> f.asInstanceOf[GreaterThanOrEqual].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[LessThan]) {
          keyCondExpr += f.asInstanceOf[LessThan].attribute + " < :" + f.asInstanceOf[LessThan].attribute
          Map(":" + f.asInstanceOf[LessThan].attribute -> f.asInstanceOf[LessThan].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[LessThanOrEqual]) {
          keyCondExpr = prevKeyCondExpr
          leAttributes += f.asInstanceOf[LessThanOrEqual].attribute
          Map(":" + f.asInstanceOf[LessThanOrEqual].attribute + "_le" -> f.asInstanceOf[LessThanOrEqual].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[StringStartsWith]) {
          keyCondExpr += "begins_with(" + f.asInstanceOf[StringStartsWith].attribute + ", :" + f.asInstanceOf[StringStartsWith].attribute + ")"
          Map(":" + f.asInstanceOf[StringStartsWith].attribute -> f.asInstanceOf[StringStartsWith].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[And] && f.asInstanceOf[And].left.isInstanceOf[LessThanOrEqual] && f.asInstanceOf[And].right.isInstanceOf[GreaterThanOrEqual] && f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute.equals(f.asInstanceOf[And].right.asInstanceOf[GreaterThanOrEqual].attribute)) {
          keyCondExpr += f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute + " between :" + f.asInstanceOf[And].right.asInstanceOf[GreaterThanOrEqual].attribute + "_left and :" + f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute + "_right"
          Map(":" + f.asInstanceOf[And].right.asInstanceOf[GreaterThanOrEqual].attribute + "_left" -> f.asInstanceOf[And].right.asInstanceOf[GreaterThanOrEqual].value.asInstanceOf[AnyRef],
            ":" + f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute + "_right" -> f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[And] && f.asInstanceOf[And].left.isInstanceOf[GreaterThanOrEqual] && f.asInstanceOf[And].right.isInstanceOf[LessThanOrEqual] && f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute.equals(f.asInstanceOf[And].right.asInstanceOf[LessThanOrEqual].attribute)) {
          keyCondExpr += f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute + " between :" + f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute + "_left and :" + f.asInstanceOf[And].right.asInstanceOf[LessThanOrEqual].attribute + "_right"
          Map(":" + f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute + "_left" -> f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].value.asInstanceOf[AnyRef],
            ":" + f.asInstanceOf[And].right.asInstanceOf[LessThanOrEqual].attribute + "_right" -> f.asInstanceOf[And].right.asInstanceOf[LessThanOrEqual].value.asInstanceOf[AnyRef])
        } else {
          keyCondExpr = prevKeyCondExpr
          Map[String, AnyRef]()
        }

      if (spec.getValueMap != null)
        spec.withValueMap((valueMap ++ spec.getValueMap.asScala).asJava)
      else
        spec.withValueMap(valueMap.asJava)
    }

    for (attr <- geAttributes) {
      if (keyCondExpr.length > 0)
        keyCondExpr += " and "
      if (leAttributes.contains(attr))
        keyCondExpr += attr + " between :" + attr + "_ge and :" + attr + "_le"
      else
        keyCondExpr += attr + " >= :" + attr + "_ge"
    }

    for (attr <- leAttributes) {
      if (!geAttributes.contains(attr)) {
        if (keyCondExpr.length > 0)
          keyCondExpr += " and "
        keyCondExpr += attr + " <= :" + attr + "_le"
      }
    }

    spec.withKeyConditionExpression(keyCondExpr)
    spec
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (filters != null && filters.length > 0) {
      val items = sqlContext.sparkContext.parallelize(List(0)).flatMap(
        dummy => {
          val table = getTable(
            tableName = tableName,
            maybeCredentials,
            maybeRegion,
            maybeEndpoint)
          val querySpec = buildQuerySpec(new QuerySpec().withMaxPageSize(pageSize).withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL), filters)
          val result = table.query(querySpec)
          val maybeRateLimiter = maybeRateLimit.map(rateLimit => {
            log.info(s"Query using rate limit of $rateLimit")
            RateLimiter.create(rateLimit)
          })
          result.pages.iterator.flatMap(page => {
            log.info("Page size: " + page.size)
            log.info("Consumed capacity: " + page.getLowLevelResult.getQueryResult.getConsumedCapacity)
            // Blocks until rate limit is available.
            maybeRateLimiter.foreach(rateLimiter => {
              // DynamoDBLocal.jar does not implement consumed capacity
              val maybeConsumedCapacityUnits = Option(page.getLowLevelResult.getQueryResult.getConsumedCapacity)
                .map(_.getCapacityUnits)
                .map(math.ceil(_).toInt)

              maybeConsumedCapacityUnits.foreach(consumedCapacityUnits => {
                log.info("Try lock according to: " + consumedCapacityUnits)
                rateLimiter.tryAcquire(consumedCapacityUnits)
              })
            })

            // This result set resides in local memory.
            page.iterator
          })
        }
      )

      val failureCounter = new AtomicLong
      val maybeSchema = Some(schema)
      items.flatMap(item => {
        try {
          Some(ItemConverter.toRow(item, maybeSchema.get))
        } catch {
          case NonFatal(err) =>
            // Log some example conversion failures but do not spam the logs.
            if (failureCounter.incrementAndGet < 3) {
              log.error(s"Failed converting item to row: ${item.toJSON}", err)
            }
            None
        }
      })

    } else {
      buildScan(requiredColumns)
    }
  }

  def scan(config: ScanConfig): Iterator[Row] = {
    val scanSpec = getScanSpec(config)
    val table = getTable(config)
    val result = table.scan(scanSpec)
    val failureCounter = new AtomicLong

    val maybeRateLimiter = config.maybeRateLimit.map(rateLimit => {
      log.info(s"Segment ${config.segment} using rate limit of $rateLimit")
      RateLimiter.create(rateLimit)
    })

    // Each `pages.next` call results in a DynamoDB network call.
    result.pages.iterator.flatMap(page => {
      log.info("Page size: " + page.size())
      // This result set resides in local memory.
      val rows = page.iterator.flatMap(item => {
        try {
          Some(ItemConverter.toRow(item, config.maybeSchema.get))
        } catch {
          case NonFatal(err) =>
            // Log some example conversion failures but do not spam the logs.
            if (failureCounter.incrementAndGet < 3) {
              log.error(s"Failed converting item to row: ${item.toJSON}", err)
            }
            None
        }
      })

      // Blocks until rate limit is available.
      maybeRateLimiter.foreach(rateLimiter => {
        // DynamoDBLocal.jar does not implement consumed capacity
        val maybeConsumedCapacityUnits = Option(page.getLowLevelResult.getScanResult.getConsumedCapacity)
          .map(_.getCapacityUnits)
          .map(math.ceil(_).toInt)

        maybeConsumedCapacityUnits.foreach(consumedCapacityUnits => {
          log.info("Try lock according to: " + consumedCapacityUnits)
          rateLimiter.tryAcquire(consumedCapacityUnits)
        })
      })

      rows
    })
  }
}
