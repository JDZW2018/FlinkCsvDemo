package org.example.flink


import org.apache.flink.api.scala._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.core.fs.FileSystem.WriteMode

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.common.operators.Order
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.CsvTableSink


object MainBak {

  //case class Point(x: Double, y: Double)
  case class Itenary(date: Long, productId: Int, eventName: String, userId: Int)
  case class Youtube(video_id: String, trending_date: String, title: String, channel_title: String,
                     category_id:String,publish_time:String,tags:String,views:String,likes:Int,
                     dislikes:Int,comment_count:String,thumbnail_link:String,comments_disabled:String,
                     ratings_disabled:String,video_error_or_removed:String)
  //video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataPath = "flinkBatchExample/data/youtube.csv"

    // read a csv file and store it as a dataset
    val ds: DataSet[Youtube] = env.readCsvFile[Youtube](
      filePath = dataPath,
      lineDelimiter = "\n",
      fieldDelimiter = ",",
      ignoreFirstLine = true)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    tableEnv.registerDataSet("youtube", ds,'video_id,'trending_date,'title,'channel_title,'category_id,'publish_time,
      'tags,'views,'likes,'dislikes,'comment_count,'thumbnail_link,'comments_disabled,'ratings_disabled,'video_error_or_removed)


    val top10_table = tableEnv.sqlQuery(" select * from (select  title,sum(pureLikes) as total_pureLikes from " +
      "(select video_id,title,(likes - dislikes) as pureLikes from youtube) t1 group by title) t2 order by total_pureLikes desc limit 10 " +
      "")
    top10_table.printSchema()

    val  top10_sink = new CsvTableSink("flinkBatchExample/output/pure-likes-top10", ",")
    top10_table.writeToSink(top10_sink)

    val  total_video = tableEnv.sqlQuery("select title  from youtube group by title ")
    val  total_video_sink = new CsvTableSink("flinkBatchExample/output/total_video", ",")
    total_video.writeToSink(total_video_sink)


    //tableEnv.sqlQuery("select ")

    //val rtnDataset = tableEnv.toDataSet[(String,Int,Int,Int)](table)

    //rtnDataset.print()

    env.execute("test hdfs csvfile")

  }
}
