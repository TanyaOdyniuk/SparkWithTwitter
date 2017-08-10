package sparktask;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.Durations;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Arrays;

/**
 * Created by Odyniuk on 21/04/2017.
 */
public final class SparkWithTwitter {

    final static String API_KEY = "fzzSKrktlcildjrF70BMqscVf";
    final static String API_SECRET = "t5cp96EZZneUqPn83HqZPcgK7ui0NQEEYM9faqk537aKn2i7HS";
    final static String ACCESS_TOKEN = "732455182940930048-3iGGfxwIxuLAvk6Zr8BQfO1xUaSwI1q";
    final static String ACCESS_TOKEN_SECRET = "pYxDsfXuVuSO42CSmAUQDnL7HEmuclZzEqFpzT7o8yu63";
    final static Long SLIDING_INTERVAL = 20L;
    final static Long WINDOW_LENGTH = 2L;
    final static String SPARK_MASTER = "local[*]";
    final static String SEPARATOR = " ";
    final static String [] FILTERS = {"#kharkov", "#kharkiv"};
    final static String QUERY = "select word, count(*) as total from words group by word order by total desc limit 10";
    static class JavaSparkSessionSingleton {
        private static transient SparkSession instance = null;
        public static SparkSession getInstance(SparkConf sparkConf) {
            if (instance == null) {
                instance = SparkSession
                        .builder()
                        .config(sparkConf)
                        .getOrCreate();
            }
            return instance;
        }
    }

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName(SparkWithTwitter.class.getName()).setMaster(SPARK_MASTER);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(SLIDING_INTERVAL));

        Configuration configuration = new ConfigurationBuilder().setDebugEnabled(false)
                .setOAuthConsumerKey(API_KEY)
                .setOAuthConsumerSecret(API_SECRET)
                .setOAuthAccessToken(ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
                .build();
        Authorization twitterAuth = new OAuthAuthorization(configuration);

        JavaDStream<Status> tweets = TwitterUtils.createStream(javaStreamingContext, twitterAuth, FILTERS);

        processing(tweets);

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

    private static void processing(JavaDStream<Status> tweets){

        JavaDStream<String> statuses = tweets
                .window(Durations.minutes(WINDOW_LENGTH), Durations.seconds(SLIDING_INTERVAL))
                .map(status -> status.getText());

        JavaDStream<Long> count = statuses.count();
        count.print();

        JavaDStream<String> words = statuses.flatMap(x -> Arrays.asList(x.split(SEPARATOR)).iterator());

        words.foreachRDD((rdd) -> {
            SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

            JavaRDD<JavaRecord> rowRDD = rdd.map(word -> {
                JavaRecord record = new JavaRecord();
                record.setWord(word);
                return record;
            });
            Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRecord.class);
            wordsDataFrame.createOrReplaceTempView("words");

            Dataset<Row> wordCountsDataFrame = spark.sql(QUERY);
            wordCountsDataFrame.show();
        });
    }
}

