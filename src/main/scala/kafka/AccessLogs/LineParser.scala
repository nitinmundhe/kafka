package kafka.AccessLogs

import java.util.regex.{Matcher, Pattern}

/**
  * A sample record:
  * 94.102.63.11 - - [21/Jul/2009:02:48:13 -0700] "GET / HTTP/1.1" 200 18209 "http://acme.com/foo.php" "Mozilla/4.0 (compatible; MSIE 5.01; Windows NT 5.0)"
  *
  */

class LineParser {

  private val ddd = "\\d{1,3}" // at least 1 but not more than 3 times (possessive)
  private val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?" // like `123.456.7.89`
  private val client = "(\\S+)" // '\S' is 'non-whitespace character'
  private val user = "(\\S+)"
  private val dateTime = "(\\[.+?\\])" // like `[21/Jul/2009:02:48:13 -0700]`
  private val request = "\"(.*?)\"" // any number of any character, reluctant
  private val status = "(\\d{3})"
  private val bytes = "(\\S+)" // this can be a "-"
  private val referer = "\"(.*?)\""
  private val agent = "\"(.*?)\""
  private val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
  private val p = Pattern.compile(regex)

  def parseLine(line: String): Option[AccessLogRecord] = {
    val matcher = p.matcher(line)
    if (matcher.find()) Some(buildAccessLogRecord(matcher)) else None
  }

  def buildAccessLogRecord(matcher: Matcher): AccessLogRecord = {
    AccessLogRecord(
      matcher.group(1),
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8),
      matcher.group(9))
  }

}
