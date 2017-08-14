package org.apache.spark.dynamodb

import java.util.concurrent.atomic.AtomicLong

import com.amazonaws.services.dynamodbv2.document.{Item, Table}
import com.amazonaws.services.dynamodbv2.document.api.QueryApi
import com.amazonaws.services.dynamodbv2.document.spec.{QuerySpec, ScanSpec}
import com.amazonaws.services.dynamodbv2.model.{KeySchemaElement, ReturnConsumedCapacity, Select}
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
          keyCondExpr += "#_" + f.asInstanceOf[EqualTo].attribute + " = :" + f.asInstanceOf[EqualTo].attribute
          Map(":" + f.asInstanceOf[EqualTo].attribute -> f.asInstanceOf[EqualTo].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[GreaterThan]) {
          keyCondExpr += "#_" + f.asInstanceOf[GreaterThan].attribute + " > :" + f.asInstanceOf[GreaterThan].attribute
          Map(":" + f.asInstanceOf[GreaterThan].attribute -> f.asInstanceOf[GreaterThan].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[GreaterThanOrEqual]) {
          keyCondExpr = prevKeyCondExpr
          geAttributes += "#_" + f.asInstanceOf[GreaterThanOrEqual].attribute
          Map(":" + f.asInstanceOf[GreaterThanOrEqual].attribute + "_ge" -> f.asInstanceOf[GreaterThanOrEqual].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[LessThan]) {
          keyCondExpr += "#_" + f.asInstanceOf[LessThan].attribute + " < :" + f.asInstanceOf[LessThan].attribute
          Map(":" + f.asInstanceOf[LessThan].attribute -> f.asInstanceOf[LessThan].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[LessThanOrEqual]) {
          keyCondExpr = prevKeyCondExpr
          leAttributes += "#_" + f.asInstanceOf[LessThanOrEqual].attribute
          Map(":" + f.asInstanceOf[LessThanOrEqual].attribute + "_le" -> f.asInstanceOf[LessThanOrEqual].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[StringStartsWith]) {
          keyCondExpr += "begins_with(#_" + f.asInstanceOf[StringStartsWith].attribute + ", :" + f.asInstanceOf[StringStartsWith].attribute + ")"
          Map(":" + f.asInstanceOf[StringStartsWith].attribute -> f.asInstanceOf[StringStartsWith].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[And] && f.asInstanceOf[And].left.isInstanceOf[LessThanOrEqual] && f.asInstanceOf[And].right.isInstanceOf[GreaterThanOrEqual] && f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute.equals(f.asInstanceOf[And].right.asInstanceOf[GreaterThanOrEqual].attribute)) {
          keyCondExpr += "#_" + f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute + " between :" + f.asInstanceOf[And].right.asInstanceOf[GreaterThanOrEqual].attribute + "_left and :" + f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute + "_right"
          Map(":" + f.asInstanceOf[And].right.asInstanceOf[GreaterThanOrEqual].attribute + "_left" -> f.asInstanceOf[And].right.asInstanceOf[GreaterThanOrEqual].value.asInstanceOf[AnyRef],
            ":" + f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute + "_right" -> f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].value.asInstanceOf[AnyRef])
        } else if (f.isInstanceOf[And] && f.asInstanceOf[And].left.isInstanceOf[GreaterThanOrEqual] && f.asInstanceOf[And].right.isInstanceOf[LessThanOrEqual] && f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute.equals(f.asInstanceOf[And].right.asInstanceOf[LessThanOrEqual].attribute)) {
          keyCondExpr += "#_" + f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute + " between :" + f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute + "_left and :" + f.asInstanceOf[And].right.asInstanceOf[LessThanOrEqual].attribute + "_right"
          Map(":" + f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute + "_left" -> f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].value.asInstanceOf[AnyRef],
            ":" + f.asInstanceOf[And].right.asInstanceOf[LessThanOrEqual].attribute + "_right" -> f.asInstanceOf[And].right.asInstanceOf[LessThanOrEqual].value.asInstanceOf[AnyRef])
        } else {
          keyCondExpr = prevKeyCondExpr
          Map[String, AnyRef]()
        }

      val nameMap = if (f.isInstanceOf[EqualTo]) Map("#_" + f.asInstanceOf[EqualTo].attribute -> f.asInstanceOf[EqualTo].attribute)
        else if (f.isInstanceOf[GreaterThan]) Map("#_" + f.asInstanceOf[GreaterThan].attribute -> f.asInstanceOf[GreaterThan].attribute)
        else if (f.isInstanceOf[GreaterThanOrEqual]) Map("#_" + f.asInstanceOf[GreaterThanOrEqual].attribute -> f.asInstanceOf[GreaterThanOrEqual].attribute)
        else if (f.isInstanceOf[LessThan]) Map("#_" + f.asInstanceOf[LessThan].attribute -> f.asInstanceOf[LessThan].attribute)
        else if (f.isInstanceOf[LessThanOrEqual]) Map("#_" + f.asInstanceOf[LessThanOrEqual].attribute -> f.asInstanceOf[LessThanOrEqual].attribute)
        else if (f.isInstanceOf[StringStartsWith]) Map("#_" + f.asInstanceOf[StringStartsWith].attribute -> f.asInstanceOf[StringStartsWith].attribute)
        else if (f.isInstanceOf[And] && f.asInstanceOf[And].left.isInstanceOf[LessThanOrEqual] && f.asInstanceOf[And].right.isInstanceOf[GreaterThanOrEqual] && f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute.equals(f.asInstanceOf[And].right.asInstanceOf[GreaterThanOrEqual].attribute)) Map("#_" + f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute -> f.asInstanceOf[And].left.asInstanceOf[LessThanOrEqual].attribute)
        else if (f.isInstanceOf[And] && f.asInstanceOf[And].left.isInstanceOf[GreaterThanOrEqual] && f.asInstanceOf[And].right.isInstanceOf[LessThanOrEqual] && f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute.equals(f.asInstanceOf[And].right.asInstanceOf[LessThanOrEqual].attribute)) Map("#_" + f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute -> f.asInstanceOf[And].left.asInstanceOf[GreaterThanOrEqual].attribute)
        else Map[String, String]()

      if (spec.getValueMap != null)
        spec.withValueMap((valueMap ++ spec.getValueMap.asScala).asJava)
      else
        spec.withValueMap(valueMap.asJava)

      if (spec.getNameMap != null)
        spec.withNameMap((nameMap ++ spec.getNameMap.asScala).asJava)
      else
        spec.withNameMap(nameMap.asJava)
    }

    for (attr <- geAttributes) {
      if (keyCondExpr.length > 0)
        keyCondExpr += " and "
      if (leAttributes.contains(attr))
        keyCondExpr += attr + " between :" + attr.substring(2) + "_ge and :" + attr.substring(2) + "_le"
      else
        keyCondExpr += attr + " >= :" + attr.substring(2) + "_ge"
    }

    for (attr <- leAttributes) {
      if (!geAttributes.contains(attr)) {
        if (keyCondExpr.length > 0)
          keyCondExpr += " and "
        keyCondExpr += attr + " <= :" + attr.substring(2) + "_le"
      }
    }

    spec.withKeyConditionExpression(keyCondExpr)
    spec
  }

  def matchKeySchema(filters: Array[Filter], keySchemas: java.util.List[KeySchemaElement]): Boolean = {
    val hashKey = keySchemas.asScala.filter("HASH" == _.getKeyType).iterator.next.getAttributeName
    val rangeKeys = keySchemas.asScala.filter("RANGE" == _.getKeyType)
    val rangeKey = if (rangeKeys.iterator.hasNext) rangeKeys.iterator.next.getAttributeName else null
    val hashKeyRefNum = filters.count(_.references.contains(hashKey))

    if (hashKeyRefNum > 0) {
      if (rangeKey != null) {
        val rangeKeyRefNum = filters.count(_.references.contains(rangeKey))
        if (rangeKeyRefNum > 0)
          true
        else
          false
      } else {
        true
      }
    } else
      false
  }

  def findQueryInf(table: Table, filters: Array[Filter]): QueryApi = {
    table.describe

    val tableDesc = table.getDescription
    if (matchKeySchema(filters, tableDesc.getKeySchema))
      table
    else {
      for (idx <- tableDesc.getLocalSecondaryIndexes.iterator)
        if (matchKeySchema(filters, idx.getKeySchema))
          return table.getIndex(idx.getIndexName)

      for (idx <- tableDesc.getGlobalSecondaryIndexes.iterator)
        if (matchKeySchema(filters, idx.getKeySchema))
          return table.getIndex(idx.getIndexName)

      null
    }
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
          val querySpec = buildQuerySpec(new QuerySpec().withMaxPageSize(pageSize).withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL).withSelect(Select.ALL_ATTRIBUTES), filters)
          val queryInf = findQueryInf(table, filters)
          if (queryInf != null) {
            val result = queryInf.query(querySpec)
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
          } else {
            log.error("There's no such query option for table: " + tableName)
            Array.empty[Item]
          }
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
