package org.apache.spark.dynamodb

import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/** Backup a DynamoDB table as JSON.
  *
  * The full table is scanned and the results are stored in the given output path.
  */
object DynamoBackupJob extends Job {
  def run(): Unit = {
  }

  private def deleteOutputPath(output: String): Unit = {
    log.info(s"Deleting existing output path $output")
    FileSystem.get(new URI(output), sc.hadoopConfiguration)
      .delete(new Path(output), true)
  }
}
