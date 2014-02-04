/**
 *
 * @author: manal.najem
 * @since: 2/2/14
 */

import java.io.{IOException, FileNotFoundException, PrintWriter}
import org.apache.log4j.Logger
import scala.collection.mutable
import scala.io.Source

object sortAndRank
{
  //val filename = "input.tsv"
  val filename = "real_input.tsv"

  final def logger = Logger.getLogger(sortAndRank.getClass)

  def output(buffer: mutable.Buffer[Record]) =
  {
    //Some(new PrintWriter("output.tsv")).foreach
    Some(new PrintWriter("real_output.tsv")).foreach
    {
      p =>
        p.write(buffer.mkString("\n")); p.close
    }
  }

  def main(args: Array[String])
  {
    try
    {
      val file = Source.fromFile(filename)
      val lines = file.getLines().map(l => Record(l))

      val records = lines.toBuffer[Record].distinct

      //Get unique timestamps converted to day format: yyyyMMdd instead of yyyyMMddHHmm
      val strippedRecords = records.transform(r => r.newRecordWithDayFormat)
      val copyRecords = strippedRecords.toBuffer

      //Count occurrences in the same day segment and sort it by timestamp, descending count and descending subscriberId
      val sortedRecords = strippedRecords.transform(r => r.newRecordWithCount(copyRecords, r)).distinct
        .sortBy(r => (r.timestamp, -r.count, -r.subscriberId))

      //Sort by timestamp, rank, subscriberId, serviceName and serviceType
      sortedRecords.transform(r => r.newRecordWithRank(sortedRecords, r)).distinct
        .sortBy(r => (r.timestamp, r.rank.substring(0, r.rank.indexOf("/")).toInt, r.subscriberId, r.serviceName, r
        .serviceType))

      //Log sorted/ranked records and output to tsv file
      logger.info(sortedRecords)
      output(sortedRecords)

      //Close the open source
      file.close()
    }
    catch
      {
        case ex: FileNotFoundException => println("Couldn't find that file.")
        case ex: IOException => println("Had an IOException trying to read that file")
      }
  }
}
