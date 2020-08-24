import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
object WordCount extends App {

  // Count languague occurence per line (article)
  def occurencesOfLang(lang: String) : (String, Long) = {

    val langCount = input
      .map(x => x.toLowerCase())
      .filter(x => x.contains(lang.toLowerCase))
      .count()
    return (lang, langCount)
  }
  // Setting Spark Conf and Context
  val conf = new SparkConf().setAppName("week1").setMaster("local[4]")
  val sc = new SparkContext(conf)

  // Reading wikipedia.dat file
  val input = sc.textFile("src/main/resources/wikipedia/wikipedia.dat").cache()

  // Count languague occurence for different languages
  val scalaWords = occurencesOfLang("scala")
  val pythonWords = occurencesOfLang("python")
  val haskellWords = occurencesOfLang("haskell")
  val javaWords = occurencesOfLang("java")

  println(">>>>>>>>>>>>>>>>>>>>>>>>>")
  println(scalaWords)
  println(pythonWords)
  println(haskellWords)
  println(javaWords)
  println("<<<<<<<<<<<<<<<<<<<<<<<<<<<")

  // Trying to rewrite previous computation using map
  val langsWords = List("scala", "python", "haskell", "java").map(x => occurencesOfLang(x)).sortBy(_._2)

  println(">>>>>>>>>>>>>>>>>>>>>>>>>")
  println(langsWords)
  println("<<<<<<<<<<<<<<<<<<<<<<<<<<<")

  Thread.sleep(60000)
}
