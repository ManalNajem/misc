import java.text.SimpleDateFormat
import scala.collection.mutable

/**
 * <p>
 * The information contained herein is the property of Myriad Group and is strictly proprietary and
 * confidential. Except as expressly authorized in writing by Myriad Group, you do not have the right
 * to use the information contained herein and you shall not disclose it to any third party.</p>
 *
 * <br>Copyright 2013 Myriad Group. All Rights Reserved.<br>
 *
 * @author: manal.najem
 * @since: 1/31/14
 */
case class Record(line: String) {
  val data = line.split("\t")
  require(data.length >= 4, "Record size should be at least 4")

  var serviceType = data(0)
  val serviceName = data(1)
  val subscriberId = data(2).toInt
  var timestamp = data(3)
  val count = if (data.length > 4) data(4).toInt else 0
  val rank = if (data.length > 5) data(5) else ""

  def date: java.util.Date = new SimpleDateFormat("yyyyMMddHHmm").parse(timestamp)
  def day: java.util.Date = new SimpleDateFormat("yyyyMMdd").parse(timestamp)
  def newRecordWithDay: Record = {
    date2day
    new Record(List(serviceType, serviceName, subscriberId, timestamp).mkString("\t"))
  }
  /*
  This function returns a modified record with the count
 */
  def newRecordWithCount(records: mutable.Buffer[Record], r: Record) : Record = {
    val count = records.count(_ == r)
    //records --= records.filter(_ == r)
    val newRecordWithCount = new Record(List(r.serviceType, r.serviceName, r.subscriberId, r.timestamp, count).mkString("\t"))

    return newRecordWithCount
  }
  /*
  This function returns a modified record with the rank
*/
  def newRecordWithRank(records: mutable.Buffer[Record], r: Record) : Record = {
    val denominator = records.count(_.day == r.day).toString
    val segment = records.filter(_.day == r.day).sortBy(r => -r.count)
    val countList = segment.map(r => r.count).distinct
    val last = countList.last
    val rank = if (r.count != last) (countList.indexOf(r.count) + 1).toString else denominator
    val nominator = rank + "/" + denominator
    val newRecordWithCount = new Record(List(r.serviceType, r.serviceName, r.subscriberId, r.timestamp, r.count, nominator).mkString("\t"))

    return newRecordWithCount
  }

  /*
  This function converts dates from yyyyMMddHHmm to yyyyMMdd format
   */
  def date2day = timestamp = timestamp.substring(0, timestamp.length - 4)

  override def toString = List(serviceType, serviceName, subscriberId, timestamp, count, rank).mkString("\t")
}

