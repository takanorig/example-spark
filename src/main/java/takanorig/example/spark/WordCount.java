package takanorig.example.spark;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * WordCount By Apache Spark
 */
public class WordCount {

    @SuppressWarnings({ "serial", "unchecked", "rawtypes" })
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("word-count").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        URL fileUrl = WordCount.class.getResource("/spark-overview.txt");
        Path filePath = Paths.get(fileUrl.toURI());

        JavaRDD<String> lines = sc.textFile(filePath.toFile().getAbsolutePath()).cache();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line) {
                String x = line.toLowerCase();
                return Arrays.asList(x.split(" "));
            }
        });

        String regex = "(^.+)(,|\\.|;|:)$";
        Pattern p = Pattern.compile(regex);
        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) {
                Matcher m = p.matcher(word);
                String x = m.replaceAll("$1");
                return new Tuple2(x, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        }).sortByKey();

        counts.foreach(val -> System.out.println(val));

        sc.close();
    }
}
