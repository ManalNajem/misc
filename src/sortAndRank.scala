/**
 *
 * @author: manal.najem
 * @since: 1/31/14
 */

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.collection.mutable
import scala.io.Source
//import java.io.{FileReader, FileNotFoundException, IOException}
import java.io._

object sortAndRank
{
  val filename = "C:\\ManalN_misc_projects\\Scala\\src\\input.tsv"
  final def logger = Logger.getLogger(sortAndRank.getClass)

  def output(buffer: mutable.Buffer[Record]) =
  {
    Some(new PrintWriter("output.tsv")).foreach{p =>
      p.write(buffer.mkString("\n")); p.close
    }
  }

  def main (args: Array[String])
  {
    val logFile = "C:\\ManalN_misc_projects\\Scala\\src\\input.tsv" // Should be some file on your system
    val sc = new SparkContext("local[4]", "TextSearch App", "C:\\ManalN_Git_projects\\spark-0.8.1-incubating\\", Seq("C:\\ManalN_misc_projects\\Scala\\out\\artifacts\\Scala_jar\\Scala.jar"))
    var logData = sc.textFile(logFile, 2).cache()
    //logData = sc.parallelize(logFile)

    try {
      /*val isr = new FileInputStream(filename)
      val file = Source.createBufferedSource(isr).mkString*/
     val file = Source.fromFile(filename)
     val lines = file.getLines().map(l => Record(l))
     //val lines = logData.map(l => Record(l))

     val records = lines.toBuffer[Record].distinct
     //val records = lines.distinct

     //val vector = Vector() ++ records
     /*val b = Vector.newBuilder[Record]
     while (records.hasNext) {
       val next: Record = records.next()
       next.date2day
       b += next
     }
     b.result*/

     //Get unique timestamps converted to days
     val strippedRecords  = records.transform(r => r.newRecordWithDay)
     val copyRecords = strippedRecords.toBuffer

     //Count occurrences in the same day segment and sort it by timestamp, descending count and descending subscriberId
     val sortedRecords  = strippedRecords.transform(r => r.newRecordWithCount(copyRecords, r)).distinct.sortBy(r => (r.timestamp, -r.count, -r.subscriberId))
     //val sortedRecords = strippedRecords.map(r => (r, strippedRecords.count(_ == r)))

     //sort by timestamp and rank
     sortedRecords.transform(r => r.newRecordWithRank(sortedRecords, r)).distinct.sortBy(r => (r.timestamp, r.rank))


     //println("Number of total records: " + sortedRecords.size)
     logger.info(sortedRecords)

     output(sortedRecords)
     file.close()
    } catch {
     case ex: FileNotFoundException => println("Couldn't find that file.")
     case ex: IOException => println("Had an IOException trying to read that file")
    }
    finally {
     //final cleanup?
    }
  }

}
