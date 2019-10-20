import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pygmalios.reactiveinflux.jawa.JavaPoint;
import com.pygmalios.reactiveinflux.jawa.JavaReactiveInfluxConfig;
import com.pygmalios.reactiveinflux.jawa.Point;
import com.pygmalios.reactiveinflux.jawa.ReactiveInfluxConfig;
import com.pygmalios.reactiveinflux.jawa.sync.JavaSyncReactiveInflux;
import com.pygmalios.reactiveinflux.jawa.sync.SyncReactiveInflux;
import com.pygmalios.reactiveinflux.jawa.sync.SyncReactiveInfluxDb;

import kafka.serializer.StringDecoder;
import scala.Tuple2;


public class TwitterDStreamTrendingHashtag
{
    
    public static void main(String[] args) throws InterruptedException
    {
        
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TweetStreamProcessing");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.milliseconds(500));
        streamingContext.sparkContext().setLogLevel("ERROR");
        
        Set<String> topics = new HashSet<>();
        topics.add("admintome-test");
        
        JavaPairInputDStream<String,String> directKafkaStream = KafkaUtils.createDirectStream(streamingContext,
                String.class, String.class, StringDecoder.class, StringDecoder.class, getKafkaParams(), topics);
        
        JavaDStream<Tweet> tweetDStream = directKafkaStream.map(MapToTweet.invoke());
        
        JavaDStream<Tuple2<String,Integer>> hashTagCountOneDStream = tweetDStream.flatMap(new FlatMapFunction<Tweet,Tuple2<String,Integer>>()
        {
            @Override
            public Iterator<Tuple2<String,Integer>> call(Tweet tweet) throws Exception
            {
                List<Tuple2<String,Integer>> list = new ArrayList();
                Pattern p = Pattern.compile("#\\w+");
                Matcher matcher = p.matcher(tweet.getText());
                while(matcher.find())
                {
                    String cleanedHashtag = matcher.group(0).trim();
                    if(cleanedHashtag != null)
                    {
                        list.add(new Tuple2<>(cleanedHashtag, 1));
                    }
                }
                return list.iterator();
            }
        });
        
        JavaPairDStream<String,Integer> hashTagPairDStream = hashTagCountOneDStream.mapToPair(MapToPair.invoke());
        
        hashTagPairDStream
                .reduceByKeyAndWindow(ReduceHashTagCount.invoke(), new Duration(300000), new Duration(5000))
                .window(new Duration(300000), new Duration(5000)).reduce(new Function2<Tuple2<String,Integer>,Tuple2<String,Integer>,Tuple2<String,Integer>>()
        {
            @Override
            public Tuple2<String,Integer> call(Tuple2<String,Integer> stringIntegerTuple2, Tuple2<String,Integer> stringIntegerTuple22) throws Exception
            {
                
                return stringIntegerTuple2._2 > stringIntegerTuple22._2 ? stringIntegerTuple2 : stringIntegerTuple22;
            }
        }).foreachRDD(new VoidFunction<JavaRDD<Tuple2<String,Integer>>>()
        {
            @Override
            public void call(JavaRDD<Tuple2<String,Integer>> tuple2JavaRDD) throws Exception
            {
                tuple2JavaRDD.foreach(new VoidFunction<Tuple2<String,Integer>>()
                {
                    @Override
                    public void call(Tuple2<String,Integer> stringIntegerTuple2) throws Exception
                    {
                        ReactiveInfluxConfig config = new JavaReactiveInfluxConfig(new URI("http://10.71.69.236:31948/"));
                        
                        try(SyncReactiveInflux reactiveInflux = new JavaSyncReactiveInflux(config, 30000))
                        {
                            // Use database "example1"
                            SyncReactiveInfluxDb db = reactiveInflux.database("twittergraph");
                            
                            // Create the "example1" database
                            //db.create();
                            
                            HashMap<String,String> tags = new HashMap<>();
                            HashMap<String,Object> fields = new HashMap<>();
                            fields.put("hashtag", stringIntegerTuple2._1);
                            fields.put("count", stringIntegerTuple2._2);
                            
                            // Write a single point to "measurement1"
                            Point point = new JavaPoint(
                                    DateTime.now(),
                                    "TrendingHashTagSpark",
                                    tags,
                                    fields
                            );
                            db.write(point);
                        }
                    }
                });
                
            }
        });
        ;
        
        // Return a new DStream of single-element RDDs by counting the number of elements in each RDD of the source DStream.
        
        JavaDStream<Long> countDStream = tweetDStream.window(new Duration(1000)).count();
        countDStream.foreachRDD(new VoidFunction<JavaRDD<Long>>()
        {
            @Override
            public void call(JavaRDD<Long> count) throws Exception
            {
                count.foreach(new VoidFunction<Long>()
                {
                    @Override
                    public void call(Long aLong) throws Exception
                    {
                        ReactiveInfluxConfig config = new JavaReactiveInfluxConfig(new URI("http://10.71.69.236:31948/"));
                        
                        try(SyncReactiveInflux reactiveInflux = new JavaSyncReactiveInflux(config, 30000))
                        {
                            SyncReactiveInfluxDb db = reactiveInflux.database("twittergraph");
                            
                            HashMap<String,String> tags = new HashMap<>();
                            HashMap<String,Object> fields = new HashMap<>();
                            fields.put("count", aLong);
                            
                            Point point = new JavaPoint(
                                    DateTime.now(),
                                    "TweetPerSecondCountSpark",
                                    tags,
                                    fields
                            );
                            db.write(point);
                        }
                    }
                });
                
            }
        });
        
        final AtomicLong runningCount = new AtomicLong(0);
        
        tweetDStream.window(new Duration(10000)).foreachRDD(new VoidFunction<JavaRDD<Tweet>>()
        {
            @Override
            public void call(JavaRDD<Tweet> tweetJavaRDD) throws Exception
            {
                runningCount.getAndAdd(tweetJavaRDD.count());
                ReactiveInfluxConfig config = new JavaReactiveInfluxConfig(new URI("http://10.71.69.236:31948/"));
                try(SyncReactiveInflux reactiveInflux = new JavaSyncReactiveInflux(config, 30000))
                {
                    SyncReactiveInfluxDb db = reactiveInflux.database("twittergraph");
                    
                    HashMap<String,String> tags = new HashMap<>();
                    HashMap<String,Object> fields = new HashMap<>();
                    fields.put("count", runningCount.get());
                    
                    Point point = new JavaPoint(
                            DateTime.now(),
                            "TotalTweetCountSpark",
                            tags,
                            fields
                    );
                    db.write(point);
                }
                
            }
        });
        
        streamingContext.start();
        
        streamingContext.awaitTermination();
    }
    
    // Misc Kafka client properties
    private static Map<String,String> getKafkaParams()
    {
        Map<String,String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.71.69.236:31440");
        kafkaParams.put("group.id", "testttt");
        //kafkaParams.put("auto.offset.reset", "smallest");
        return kafkaParams;
    }
    
    private static class MapToTweet
    {
        private static Function<Tuple2<String,String>,Tweet> invoke()
        {
            return new Function<Tuple2<String,String>,Tweet>()
            {
                @Override
                public Tweet call(Tuple2<String,String> stringStringTuple2) throws Exception
                {
                    ObjectMapper mapper = new ObjectMapper();
                    try
                    {
                        return mapper.readValue(stringStringTuple2._2, Tweet.class);
                    }
                    catch(Exception e)
                    {
                        // That's ok, received a malformed document
                    }
                    return null;
                }
            };
        }
    }
    
    private static class MapToPair
    {
        private static PairFunction<Tuple2<String,Integer>,String,Integer> invoke()
        {
            return new PairFunction<Tuple2<String,Integer>,String,Integer>()
            {
                @Override
                public Tuple2<String,Integer> call(Tuple2<String,Integer> stringIntegerTuple2) throws Exception
                {
                    return stringIntegerTuple2;
                }
            };
        }
    }
    
    private static class ReduceHashTagCount
    {
        private static Function2<Integer,Integer,Integer> invoke()
        {
            return new Function2<Integer,Integer,Integer>()
            {
                @Override
                public Integer call(Integer a, Integer b)
                {
                    return a + b;
                }
            };
        }
    }
}
