/**
 * <p>
 * The information contained herein is the property of Myriad Group and is strictly proprietary and
 * confidential. Except as expressly authorized in writing by Myriad Group, you do not have the right
 * to use the information contained herein and you shall not disclose it to any third party.</p>
 *
 * <br>Copyright 2013 Myriad Group. All Rights Reserved.<br>
 *
 * @author: manal.najem
 * @since: 1/30/14
 */

import org.apache.spark.SparkContext

object TextSearch
{
  def main(args: Array[String])
  {
    val logFile = "C:\\ManalN_misc_projects\\Scala\\src\\input.tsv" // Should be some file on your system
  val sc = new SparkContext("local[4]", "TextSearch App", "C:\\ManalN_Git_projects\\spark-0.8.1-incubating\\",
                            Seq("C:\\ManalN_misc_projects\\Scala\\out\\artifacts\\Scala_jar\\Scala.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    //val logData = sc.parallelize(logFile)

    val numOs = logData.filter(line => line.contains("o")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with o: %s, Lines with b: %s".format(numOs, numBs))
  }
}
