import util.{CommandLineOptions, FileUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/** Inverted Index - Basis of Search Engines */
object InvertedIndex5b {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("output/crawl"),
      CommandLineOptions.outputPath("output/inverted-index"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet)

    val argz   = options(args.toList)
    val master = argz("master")
    val quiet  = argz("quiet").toBoolean
    val out    = argz("output-path")
    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }

    val name = "Inverted Index (5b)"
    val spark = SparkSession.builder.
      master(master).
      appName(name).
      config("spark.app.id", name).   // To silence Metrics warning.
      getOrCreate()
    val sc = spark.sparkContext

    try {
      // Load the input "crawl" data, where each line has the format:
      //   (document_id, text)
      // First remove the outer parentheses, split on the first comma,
      // trim whitespace from the name (we'll do it later for the text)
      // and convert the text to lower case.
      // NOTE: The args("input-path").toString is a directory; Spark finds the correct
      // data files, part-NNNNN.
      val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
      val input = sc.textFile(argz("input-path")).map {
        case lineRE(name, text) => (name.trim, text.toLowerCase)
        case badLine =>
          Console.err.println(s"Unexpected line: $badLine")
          // If any of these were returned, you could filter them out below.
          ("", "")
      }  // RDD[(String,String)] of (path,text) pairs

      if (!quiet) println(s"Writing output to: $out")

      val numDocuments = input.count
      val tf = input
        .flatMap {
          case (path, text) =>
            val doc = text.trim.split("""[^\p{IsAlphabetic}]+""")
            doc
              .groupBy(words => words)
              .mapValues(_.length)
              .toSeq
              .map{ case (word, count) => (word, (path, (count.toDouble/doc.length)))}
        }

      val idf = input
          .flatMap {
            case (path, text) =>
              val doc = text.trim.split("""[^\p{IsAlphabetic}]+""")
              doc
                .toSet[String]
                .map(word => (word, path))
          }
        .groupByKey()
        .map {
          case (word, path) => (word, Math.log(numDocuments/path.size))
        }

      tf.join(idf)
        .map {
          case (word, ((path, tfVal), idfVal)) =>
            (word, (path, tfVal * idfVal))
        }
        .saveAsTextFile(out)
    } finally {
      // This is a good time to look at the web console again:
      if (! quiet) {
        println("""
          |========================================================================
          |
          |    Before closing down the SparkContext, open the Spark Web Console
          |    http://localhost:4040 and browse the information about the tasks
          |    run for this example.
          |
          |    When finished, hit the <return> key to exit.
          |
          |========================================================================
          """.stripMargin)
        Console.in.read()
      }
      spark.stop()
    }

    // Exercise: Sort the output by the words. How much overhead does this add?
    // Exercise: For each output record, sort the list of (path, n) tuples by n,
    //   descending. Recall that you would want a real search index to show you
    //   the documents first that have a lot to say about the subject.
    // Exercise: Try you own set of text files. First run Crawl5a to generate
    //   the "web crawl" data.
    // Exercise (hard): Try combining some of the processing steps or reordering
    //   steps to make it more efficient.
    // Exercise (hard): As written the output data has an important limitation
    //   for use in a search engine. Really common words, like "a", "an", "the",
    //   etc. are pervasive. There are two tools to improve this. One is to
    //   filter out so-called "stop" words that aren't useful for the index.
    //   The second is to use a variation of this algorithm called "term
    //   frequency-inverse document frequency" (TF-IDF). Look up this algorithm
    //   and implement it.
  }
}
