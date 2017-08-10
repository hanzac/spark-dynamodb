package org.apache.spark.dynamodb

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

/** Base class for Spark jobs. */
trait Job {
  protected val log = LoggerFactory.getLogger(this.getClass)
  protected val name: String = getClass.getName.stripSuffix("$")

  lazy val conf = new SparkConf().setAppName(getClass.getName)
  lazy implicit val sc = SparkContext.getOrCreate(conf)

  /** Users should override this method with their Spark job logic. */
  def run(): Unit

  def main(args: Array[String]): Unit = {
    try {
      run()
    } finally {
      sc.stop()
    }
  }
}
