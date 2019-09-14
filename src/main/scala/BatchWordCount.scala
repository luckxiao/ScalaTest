import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //文件输入的路径
    val inputData = "E:\\BatchData\\file.txt"
    //文件输出
    val outputData = "E:\\BatchData\\result"
    //获取环境变量
    val env = ExecutionEnvironment.getExecutionEnvironment
    //读取文件中的内容
    val text = env.readTextFile(inputData)
    import org.apache.flink.api.scala._
    // 先将大写字母转化为小写，然后再使用分隔符
    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)   // 过滤掉空的字符串
      .map((_,1))  // 这里是简写 .map(w =>WordWithCount(w,1))
      .groupBy(0)
      .sum(1)
    counts.writeAsCsv(outputData,"\n"," ").setParallelism(1)
    env.execute("batch word count")
  }
}
