package at.jku.dke.kschuetz

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.time.LocalDate
import java.util.regex.Pattern

object EmailGeneratorApp{
  val spark = SparkSession
    .builder
    .master("local")
    .appName("Structured Streaming")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    // Read from the text stream and convert it into a Dataset[Email]
    val emailsDF = spark.readStream
      .option("wholetext", "true")
      .text("C:\\Users\\dke\\Documents\\dwh spark\\data\\mails\\generated")
    // Convert content to email
    val emailsDS = emailsDF
      .map(row => MailParser.parseMail(row.getString(0), s => LocalDateParserUtil.parseLocalDateStreamVersion(s)))(Encoders.product[Email])
      .toDF("id", "date", "from", "recipients", "subject", "body")

     // Add columns
    val sourceDF = emailsDS
      .withColumn("length", size(split(emailsDS.col("body"), " ")))
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", "1 minutes")
      .toDF("id", "date", "from", "recipients", "subject", "body", "length", "timestamp")

    val topSenderDS = sourceDF
      .groupBy($"from", $"timestamp")
      .agg(
        "*" -> "count",
        "length" -> "sum"
      )

    val query = topSenderDS.writeStream //
      .option("path", "C:\\Users\\dke\\Documents\\dwh spark\\data\\json\\structured_streaming\\file_sink")
      .option("checkpointLocation", "C:\\Users\\dke\\Documents\\dwh spark\\data\\json\\structured_streaming\\checkpoint")
      .outputMode("append")
      .format("json")
      .start()

    query.awaitTermination();
  }
}
case class TopStatistics(timestamp: LocalDate, topFiveSendersByMails: Array[String], topFiveSendersByWord: Array[String], mostUsedWords: Array[String])