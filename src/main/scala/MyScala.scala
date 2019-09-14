import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object MyScala {
  // Data type for words with count
  case class WordWithCount(word: String, count: Long)
  def main(args: Array[String]): Unit = {
    //获取port
    val port:Int = try {
    ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case  e:Exception=>{
        System.out.println("no port")
      }
        8889   //失败使用默认值
    }

    // 获取执行的环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //连接socekt 获取输入数据

    //获取数据: 从socket中获取
    val textDataStream = env.socketTextStream("localhost", port, '\n')

    // 解析数据(把数据打平)，分组，窗口计算并且
    val WindowCounts = textDataStream.flatMap(line => line.split("\\s")) // 打平
    .map(w =>WordWithCount(w,1))  // 把单词转化为word
      .keyBy("word") //分组
      .timeWindow(Time.seconds(2),Time.seconds(1))  //指定窗口的大小和间隔时间
      .sum("count")  //聚合
    // 打印到控制台
    WindowCounts.print().setParallelism(1)
    //启动执行器，执行程序
    env.execute("Socket Window WordCount")
  }

}
