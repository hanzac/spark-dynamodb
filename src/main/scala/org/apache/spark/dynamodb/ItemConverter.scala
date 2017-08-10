package org.apache.spark.dynamodb

import com.amazonaws.services.dynamodbv2.document.Item
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

private[dynamodb] object ItemConverter {
  private val log = LoggerFactory.getLogger(this.getClass)
  private implicit val Formats = DefaultFormats

  /** Item to Row converter.
    *
    * Note this is a simple converter for use in tests while determining the overall structure,
    * and does not focus on efficiency or handling all data types at this time.
    */
  def toRow(item: Item, schema: StructType): Row = {
    val json = parse(item.toJSON)

    val values: Seq[Any] = schema.map(field => {
      val jsonFieldValue = json \ field.name

      jsonFieldValue match {
        case JNothing =>
          // item does not have a value for this field
          // scalastyle:off null
          null
          // scalastyle:on null
        case _ =>
          field.dataType match {
            case IntegerType => jsonFieldValue.extract[Int]
            case LongType => jsonFieldValue.extract[Long]
            case DoubleType => jsonFieldValue.extract[Double]
            case StringType => jsonFieldValue.extract[String]
          }
      }
    })

    Row.fromSeq(values)
  }
}
