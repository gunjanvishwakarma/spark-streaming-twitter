import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple3;


public class TwitterDatasetTrendingHashtag
{
    
    public static void main(String[] args) throws InterruptedException, StreamingQueryException
    {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TweetStreamProcessing");
        conf.set("spark.sql.codegen.wholeStage", "false");
        SparkSession spark = new SparkSession(new SparkContext(conf));
        spark.sparkContext().setLogLevel("ERROR");
        Set<String> topics = new HashSet<>();
        topics.add("admintome-test");
        
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.71.69.236:31440")
                .option("subscribe", "admintome-test")
                .load();
        
        Dataset<Tuple3<String,Integer,Timestamp>> tweetDataset = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").map(new MapFunction<Row,Tweet>()
        {
            @Override
            public Tweet call(Row row) throws Exception
            {
                return new ObjectMapper().readValue(row.getString(1), Tweet.class);
            }
        }, Encoders.bean(Tweet.class))
                
                .flatMap(new FlatMapFunction<Tweet,Tuple3<String,Integer,Timestamp>>()
                {
                    @Override
                    public Iterator<Tuple3<String,Integer,Timestamp>> call(Tweet row) throws Exception
                    {
                        List<Tuple3<String,Integer,Timestamp>> list = new ArrayList();
                        Pattern p = Pattern.compile("#\\w+");
                        Matcher matcher = p.matcher(row.getText());
                        while(matcher.find())
                        {
                            String cleanedHashtag = matcher.group(0).trim();
                            if(cleanedHashtag != null)
                            {
                                list.add(new Tuple3<>(cleanedHashtag, 1, new Timestamp(row.getTimestamp_ms())));
                            }
                        }
                        return list.iterator();
                    }
                }, Encoders.tuple(Encoders.STRING(), Encoders.INT(), Encoders.TIMESTAMP()));
        
        Dataset<Row> max = tweetDataset
                .withWatermark("_3", "10 seconds")
                .groupBy(functions.window(col("_3"), "300 seconds", "1 seconds"), col("_1")).sum().sort(desc("sum(_2)"));
        
        max.writeStream().format("console").outputMode("complete").start().awaitTermination();
    }
    
}