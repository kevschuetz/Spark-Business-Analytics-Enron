package at.jku.dke.kschuetz

import java.time.LocalDate
import java.util.regex.Pattern

object MailParser {
  def parseMail(content:String, mapFunction: String => LocalDate): Email = {
    val emailRegex = "^([\\w-]+): (.*)$" // matches basically any string with a colon
    val emailPattern: Pattern = Pattern.compile(emailRegex)

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
        val key = matcher.group(1) // before colon
        val value = matcher.group(2) // after colon
        key match {
          case "Message-ID" => id = value.hashCode
          case "Date" => date = mapFunction.apply(value)
          case "From" => from = value
          case "Subject" => subject = value
          case "To" | "X-To" | "X-cc" | "X-bcc" | "Cc" | "Bcc" => toList = value :: toList
          case "Mime-Version" | "Content-Type" | "Content-Transfer-Encoding" | "X-Folder" | "X-Origin" | "Time" | "Extension" | "X-From" | "X-FileName" => Unit
          case _ => body += line + "\n"
        }
      } else {
        body += line + "\n"
      }
    }

    toList = toList.filter(s => s.trim.nonEmpty) // remove blank entries in recipients
    Email(id, date, from, toList.toArray, subject, body)
  }

  def createExtendedMail(mail: Email): ExtendedEmail = {
    ExtendedEmail(mail.id, mail.date, mail.from, mail.recipients, mail.subject, mail.body, mail.body.split(" ").length)
  }
}

case class ExtendedEmail(val id: Int, val date: LocalDate,  val from: String,  val recipients: Array[String], val subject: String, val body: String, length: Int) {

}
