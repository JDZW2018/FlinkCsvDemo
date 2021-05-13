package org.example.flink


import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.CsvTableSink


object Main {

  //case class Point(x: Double, y: Double)
  case class Itenary(date: Long, productId: Int, eventName: String, userId: Int)
  case class Youtube(video_id: String, trending_date: String, title: String, channel_title: String,
                     category_id:String,publish_time:String,tags:String,views:Int,likes:Int,
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


    //所有影片名录
    val  total_video = tableEnv.sqlQuery("select title  from youtube group by title ")
    val  total_video_sink = new CsvTableSink("flinkBatchExample/output/total_video", ",")
    total_video.writeToSink(total_video_sink)

    //所有频道的统计
    val  total_channel =tableEnv.sqlQuery("select channel_title from youtube group by channel_title")
    val  total_channel_sink = new CsvTableSink("flinkBatchExample/output/total_channel", ",")
    total_channel.writeToSink(total_channel_sink)

    //纯点赞量最多的十个影片 top10
    val top10_title = tableEnv.sqlQuery(" select * from (select  title,sum(pureLikes) as total_pureLikes from " +
      "(select video_id,title,(likes - dislikes) as pureLikes from youtube) t1 group by title) t2 " +
      "order by total_pureLikes desc limit 10 " )
    val  top10_title_sink = new CsvTableSink("flinkBatchExample/output/pure-likes-top10", ",")
    top10_title.writeToSink(top10_title_sink)

    //频道纯点赞最多的十个频道 top10

    val top10_channel_total_pureLikes = tableEnv.sqlQuery(" select * from (select  channel_title,sum(pureLikes) as total_pureLikes from " +
      "(select video_id,channel_title,(likes - dislikes) as pureLikes from youtube) t1 group by channel_title) t2 " +
      "order by total_pureLikes desc limit 10 " )
    top10_channel_total_pureLikes.printSchema()

    val  top10_channel_total_pureLikes_sink = new CsvTableSink("flinkBatchExample/output/top10_channel_total_pureLikes", ",")
    top10_channel_total_pureLikes.writeToSink(top10_channel_total_pureLikes_sink)

    //播放量最多的十个影片 top10
    val top10_views_video = tableEnv.sqlQuery(" select * from " +
      "(select  title,sum(views) as total_views from " +
      "(select video_id,title,views from youtube) t1 group by title)" +
      " t2 " +
      "order by total_views desc limit 10 ")
    top10_views_video.printSchema()
    val  top10_views_video_sink = new CsvTableSink("flinkBatchExample/output/top10_views_video", ",")
    top10_views_video.writeToSink(top10_views_video_sink)

    //播放量最多的十个频道 top10
    val top10_views_channel = tableEnv.sqlQuery(" select * from " +
      "(select  channel_title,sum(views) as total_views from " +
      "(select video_id,channel_title,views from youtube) t1 group by channel_title)" +
      " t2 " +
      "order by total_views desc limit 10 ")
    top10_views_channel.printSchema()
    val  top10_views_channel_sink = new CsvTableSink("flinkBatchExample/output/top10_views_channel", ",")
    top10_views_channel.writeToSink(top10_views_channel_sink)


    //最受欢迎喜欢的10个影片，算法：pure like/播放量 和 pure like 的倒叙


    //哪些标签最吸引客户，标签拆分统计，单标签播放量 group by desc top10



    //tableEnv.sqlQuery("select ")

    //val rtnDataset = tableEnv.toDataSet[(String,Int,Int,Int)](table)

    //rtnDataset.print()

    env.execute("test hdfs csvfile")

  }
}
