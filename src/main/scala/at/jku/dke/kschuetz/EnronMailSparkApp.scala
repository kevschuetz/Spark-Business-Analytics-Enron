package at.jku.dke.kschuetz

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.regex.Pattern

object EnronMailSparkApp {
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Enron Analytics")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    var emails = loadEmails(args(0)) // load Dataset<Email> based on source emails
    writeInParquet(emails, args(1)) // write emails to parquet file
    emails = readParquetMails(args.slice(2, args.length - 2)) // combine several parquet files to dataset of emails
    writeAverageLengthAndRecipientsInInterval(args.slice(2, args.length - 2), LocalDate.MIN, LocalDate.MAX, args(args.length - 2)) // write average length and number of recipients to josn
    writeAverageLengthAndRecipientsBySenderInInterval(args.slice(2, args.length - 2), LocalDate.MIN, LocalDate.MAX, args(args.length - 1)) // write average length and number of recipients by sender to json
    spark.stop()
  }

  def readParquetMails(paths: Array[String]): Dataset[Email] = {
    paths.map(readMails).reduce(_ union _)
  }

  def readMails(path: String): Dataset[Email] = {
    spark.read.parquet(path).as[Email]
  }

  def writeAverageLengthAndRecipientsInInterval(paths: Array[String], from: LocalDate, to: LocalDate, targetPath: String): Unit = {
    val emailDS = readParquetMails(paths).filter(mail => (mail.date.isEqual(from) || mail.date.isAfter(from)) && mail.date.isBefore(to))

    val emailLengthDF = emailDS.map(mail => {
      (mail.id, mail.body.split(" ").length, mail.recipients.length)
    }).toDF("email_id", "email_length", "recipients_length")
    emailLengthDF.createOrReplaceTempView("email_view")

    val queryResult = spark.sql("SELECT avg(email_length) as avgLength, avg(recipients_length) as avgNoOfRecipients, current_timestamp as timestamp FROM email_view")
    queryResult.write.format("json").save(targetPath)
  }

  def writeAverageLengthAndRecipientsBySenderInInterval(paths: Array[String], from: LocalDate, to: LocalDate, targetPath: String): Unit = {
    val emailDS = readParquetMails(paths).filter(mail => (mail.date.isEqual(from) || mail.date.isAfter(from)) && mail.date.isBefore(to))

    val emailLengthDF = emailDS.map(mail => {
      (mail.id, mail.body.split(" ").length, mail.recipients.length, mail.from)
    }).toDF("email_id", "email_length", "recipients_length", "sender")
    emailLengthDF.createOrReplaceTempView("email_by_sender_view")

    val queryResult = spark.sql("SELECT avg(email_length) as avgLength, avg(recipients_length) as avgNoOfRecipients, current_timestamp as timestamp, sender as sender FROM email_by_sender_view GROUP BY sender")
    val statisticEntryDS = queryResult.map(row =>
      StatisticEntry(row.getAs[String]("sender"), row.getAs[Double]("avgLength"), row.getAs[Double]("avgNoOfRecipients"))
    )
    val currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    val statistics = Statistics(currentTimestamp, statisticEntryDS.collect())
    spark.createDataset[Statistics](Seq(statistics)).write.format("json").save(targetPath)

  }

  def loadEmails(path: String): Dataset[Email] = {
    val emailRegex = "^([\\w-]+): (.*)$"
    val emailPattern = Pattern.compile(emailRegex)

    val emails = spark.sparkContext
      .wholeTextFiles(path, -1)
      .flatMap { case (_, content) =>
        val lines = content.split("\n")
        var toList = List[String]()
        var id: Int = -1
        var date: LocalDate = null
        var from: String = ""
        var subject: String = ""
        var body: String = ""

        for {
          line <- lines
        } yield {
          val matcher = emailPattern.matcher(line)
          if (matcher.find()) {
            val key = matcher.group(1)
            val value = matcher.group(2)
            key match {
              case "Message-ID" => id = value.hashCode
              case "Date" => date = LocalDateParserUtil.parseLocalDate(value)
              case "From" => from = value
              case "Subject" => subject = value
              case "To" | "X-To" | "X-cc" | "X-bcc" | "Cc" | "Bcc" => toList = value :: toList
              case "Mime-Version" | "Content-Type" | "Content-Transfer-Encoding" | "X-Folder" | "X-Origin" | "Time" | "Extension" | "X-From" | "X-FileName" => Unit
              case _ => body += line + "\n"
            }
          }else{
            body += line + "\n"
          }
        }

        toList = toList.filter(s => s.trim.nonEmpty)
        val email = Email(id, date, from, toList.toArray, subject, body)
        List(email).iterator
      }
    spark.createDataset[Email](emails)
  }

  def writeInParquet(ds: Dataset[Email], path: String): Unit = {
    ds.write.format("parquet").save(path)
  }
}

case class Email(id: Int, date: LocalDate, from: String, recipients: Array[String], subject: String, body: String){
  require(from != null, "From field cannot be null")
  require(recipients != null && !recipients.isEmpty, "Recipients cannot be empty")
  require(date != null, "Date cannot be null")
}

case class StatisticEntry(sender: String, avgNoOfRecipients: Double, avgLength: Double)
case class Statistics(timestamp: String, statistics: Seq[StatisticEntry])











