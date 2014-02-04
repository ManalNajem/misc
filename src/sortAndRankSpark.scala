import java.io.{IOException, FileNotFoundException, PrintWriter}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.collection.mutable

/**
 *
 * @author: manal.najem
 * @since: 2/3/14
 */
object sortAndRankSpark
{
  final def logger = Logger.getLogger(sortAndRank.getClass)

  def output(buffer: mutable.Buffer[Record]) =
  {
    Some(new PrintWriter("real_output_spark.tsv")).foreach{p =>
    //Some(new PrintWriter("output.tsv")).foreach{p =>
      p.write(buffer.mkString("\n")); p.close
      }
  }

  def main (args: Array[String])
  {
    //val logFile = "input.tsv"
    val logFile = "real_input.tsv"
    //TODO  Adjust the parameters here
    val sc = new SparkContext("local[4]", "TextSearch App", "C:\\ManalN_Git_projects\\spark-0.8.1-incubating\\",
                              Seq("C:\\ManalN_misc_projects\\Scala\\out\\artifacts\\Scala_jar\\Scala.jar"))
    //The first time the RDD is computed in an action, it will be kept in memory on the nodes
    val logData = sc.textFile(logFile, 2).cache()

    try {
      val lines = logData.map(l => Record(l))

      val records = lines.distinct.toArray.toBuffer

      //Get unique timestamps converted to day format: yyyyMMdd instead of yyyyMMddHHmm
      val strippedRecords  = records.map(r => r.newRecordWithDayFormat)
      val copyRecords = strippedRecords.toBuffer

      //Count occurrences in the same day segment and sort it by timestamp, descending count and descending subscriberId
      val sortedRecords = strippedRecords.map(r => r.newRecordWithCount(copyRecords, r)).distinct
        .sortBy(r => (r.timestamp, -r.count, -r.subscriberId))

      //Sort by timestamp, rank, subscriberId, serviceName and serviceType
      val rankedRecords = sortedRecords.map(r => r.newRecordWithRank(sortedRecords, r)).distinct
        .sortBy(r => (r.timestamp, r.rank.substring(0, r.rank.indexOf("/")).toInt, r.subscriberId, r.serviceName, r
        .serviceType))


      //Log sorted/ranked records and output to tsv file
      logger.info(rankedRecords)
      output(rankedRecords)
    } catch {
      case ex: FileNotFoundException => println("Couldn't find that file.")
      case ex: IOException => println("Had an IOException trying to read that file")
    }
  }
}
