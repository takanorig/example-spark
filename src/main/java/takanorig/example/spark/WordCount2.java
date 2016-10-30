package takanorig.example.spark;

import java.io.Serializable;
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
 * 
 * Occur : java.io.NotSerializableException: org.apache.spark.api.java.JavaSparkContext
 */
public class WordCount2 implements Serializable {

    private static final long serialVersionUID = 1L;

    private JavaSparkContext sc;

    public WordCount2(JavaSparkContext sc) {
        this.sc = sc;
    }

    @SuppressWarnings({ "serial", "unchecked", "rawtypes" })
    public JavaPairRDD<String, Integer> count(Path filePath) {
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

        return counts;
    }

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("word-count").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        URL fileUrl = WordCount2.class.getResource("/spark-overview.txt");
        Path filePath = Paths.get(fileUrl.toURI());

        WordCount2 counter = new WordCount2(sc);
        JavaPairRDD<String, Integer> counts = counter.count(filePath);

        counts.foreach(val -> System.out.println(val));

        sc.close();
    }
}
