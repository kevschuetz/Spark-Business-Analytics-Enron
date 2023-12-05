package at.jku.dke.kschuetz

import org.apache.spark.sql.{Dataset, SparkSession}

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

  /**
   * Create a function that takes as input an array of strings that specify locations of parquet files
   *  with e-mail data and returns a single, integrated Dataset<Email> as result. In Python, the
   *  result of the function should be an RDD of e-mails.
   */
  def readParquetMails(paths: Array[String]): Dataset[Email] = {
    paths.map(readMails).reduce(_ union _)
  }

  def readMails(path: String): Dataset[Email] = {
    spark.read.parquet(path).as[Email]
  }

  /**
   * Create a function that computes the average length and the average number of recipients
   *  over all e-mails in a set of parquet files, the set of parquet files specified as an array of
   *  strings, with a date value that falls within a specified interval following these steps:
   *  a. Load a Dataset<Email> from parquet files that contain the e-mail data using the
   *  functions from the previous task.
   *  b. Filter the records by date so that only the e-mails in the given interval remain.
   *  c. From the filtered Dataset<Email>, create a new Dataset<Row> that represents a
   *  table with columns for the e-mail id and the length (number of words) of the e-mail.
   *  Register the data frame as a temporary table.
   *  d. Using Spark SQL, calculate the average number of recipients and the average mes-
   *  sage length of e-mails.
   *  e. Return the result as a JSON file with three fields: timestamp, avgLength and
   *  avgNoOfRecipients. The timestamp should store the time when the function
   *  was invoked. The JSON file should be written at a specified location.
   *
   * @param paths
   * @param from
   * @param to
   * @param targetPath
   */
  def writeAverageLengthAndRecipientsInInterval(paths: Array[String], from: LocalDate, to: LocalDate, targetPath: String): Unit = {
    val timestamp = LocalDateTime.now()
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                                               
    val emailDS = readParquetMails(paths)
      .filter(mail =>
        (mail.date.isEqual(from) || mail.date.isAfter(from)) && mail.date.isBefore(to)
      )


    // Convert dataset to new schema with required columns and register view
    val emailLengthDF = emailDS.map(mail => {
      (mail.id, mail.body.split(" ").length, mail.recipients.length)
    }).toDF("email_id", "email_length", "recipients_length")

    emailLengthDF.createOrReplaceTempView("email_view")
    // Execute query to obtain averages
    val queryResult = spark.sql("SELECT avg(email_length) as avgLength, avg(recipients_length) as avgNoOfRecipients FROM email_view")
    // Construct bean and create dataset
    val averageLengthBean = AverageLength(timestamp, queryResult.first().getAs[Double]("avgLength"), queryResult.first().getAs[Double]("avgNoOfRecipients"))
    spark.createDataset[AverageLength](Seq(averageLengthBean)).write.format("json").save(targetPath)
  }

  /**
   * Create a function that computes the average number of recipients of an e-mail for each
   *  sender as well as the average length of an e-mail for each sender from all the e-mails in a
   *  set of parquet files with a date value in a given interval following these steps:
   *  a. Load a Dataset<Email> from parquet files that contain the e-mail data using the
   *  functions from the previous task.
   *  b. Filter the records by date so that only the e-mails in the given interval remain.
   *  c. From the filtered Dataset<Email>, create a new Dataset<Row> for a table with
   *  columns for the e-mail id, the sender (From), the length (number of words) of the e-
   *  mail, and the number of recipients. Register the data frame as a temporary table.
   *  d. Using Spark SQL, calculate the average number of recipients of an e-mail for each
   *  sender as well as the average length of an e-mail for each sender.
   *  e. Return the result as a JSON file with a field for the timestamp and an array of ob-
   *  jects statistics consisting of fields for the sender, avgNoOfRecpients, and
   *  avgLength. The timestamp should store the time when the function was invoked.
   *  The JSON file should be written at a specified location.
   *
   * @param paths
   * @param from
   * @param to
   * @param targetPath
   */
  def writeAverageLengthAndRecipientsBySenderInInterval(paths: Array[String], from: LocalDate, to: LocalDate, targetPath: String): Unit = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val emailDS = readParquetMails(paths).filter(mail => (mail.date.isEqual(from) || mail.date.isAfter(from)) && mail.date.isBefore(to))
    // Convert dataset to schema with required columns
    val emailLengthDF = emailDS.map(mail => {
      (mail.id, mail.body.split(" ").length, mail.recipients.length, mail.from)
    }).toDF("email_id", "email_length", "recipients_length", "sender")
    emailLengthDF.createOrReplaceTempView("email_by_sender_view")
    // Obtain averages for each sender
    val queryResult = spark.sql("SELECT avg(email_length) as avgLength, avg(recipients_length) as avgNoOfRecipients, current_timestamp as timestamp, sender as sender FROM email_by_sender_view GROUP BY sender")
    // Map each row to bean
    val statisticEntryDS = queryResult.map(row =>
      StatisticEntry(row.getAs[String]("sender"), row.getAs[Double]("avgLength"), row.getAs[Double]("avgNoOfRecipients"))
    )
   
    // Construct result including timestamp and list of averages for each sender
    val statistics = Statistics(timestamp, statisticEntryDS.collect())
    spark.createDataset[Statistics](Seq(statistics)).write.format("json").save(targetPath)

  }

  /**
   * Create a function that takes a path (string) as input, reads all e-mail files, and returns a da-
   * taset of Email objects. You can assume that the path contains the e-mail files, and that the
   * e-mail files are not located in subdirectories of the specified path (if they are, these files are
   * not loaded by the function).
   * Note: The recipients field subsumes an e-mail’s To, Cc, and Bcc as well X-To, X-cc, and
   * X-bcc fields. The e-mail body may run over multiple lines; preserve the line breaks. Trans-
   * form the e-mail’s date field into a suitable date/time type.
   *
   * @param path
   */
  def loadEmails(path: String): Dataset[Email] = {
    val emails = spark.sparkContext
      .wholeTextFiles(path, -1) // RDD[Tuple<String,String>]
      .flatMap { case (_, content) => // content -> file-content
        val email = MailParser.parseMail(content, s => LocalDateParserUtil.parseLocalDateEnronVersion(s))
        List(email).iterator
      }
    spark.createDataset[Email](emails)
  }

  /**
   * Create a method that takes as inputs a Dataset<Email> and a path (as string), and
   *  stores the input dataset in parquet format at the specified location. In Python, you should
   *  use RDDs instead of Datasets.
   *
   * @param ds
   * @param path
   */
  def writeInParquet(ds: Dataset[Email], path: String): Unit = {
    ds.write.format("parquet").save(path)
  }
}

case class Email(id: Int, date: LocalDate, from: String, recipients: Array[String], subject: String, body: String){
  // Note: deactivated for Structured Streaming exercise, as the email generator may generate emails without recipients e.g.
//  require(from != null, "From field cannot be null " + this.toString)
//  require(recipients != null && !recipients.isEmpty, "Recipients cannot be empty" + this.toString)
//  require(date != null, "Date cannot be null" + this.toString)
}

case class AverageLength(timestamp: String, avgLength: Double, avgNoOfRecipients: Double)
case class StatisticEntry(sender: String, avgNoOfRecipients: Double, avgLength: Double)
case class Statistics(timestamp: String, statistics: Seq[StatisticEntry])











