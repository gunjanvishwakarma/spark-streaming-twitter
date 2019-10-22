import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pygmalios.reactiveinflux.jawa.JavaPoint;
import com.pygmalios.reactiveinflux.jawa.JavaReactiveInfluxConfig;
import com.pygmalios.reactiveinflux.jawa.Point;
import com.pygmalios.reactiveinflux.jawa.ReactiveInfluxConfig;
import com.pygmalios.reactiveinflux.jawa.sync.JavaSyncReactiveInflux;
import com.pygmalios.reactiveinflux.jawa.sync.SyncReactiveInflux;
import com.pygmalios.reactiveinflux.jawa.sync.SyncReactiveInfluxDb;

import scala.Tuple2;
import scala.Tuple3;


public class TwitterDatasetTrendingHashtag
{
    
    public static void main(String[] args) throws InterruptedException, StreamingQueryException
    {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("TweetStreamProcessing")
                .config("spark.sql.codegen.wholeStage", "false")
                .getOrCreate();
        
        //spark.sparkContext().setLogLevel("ERROR");
        
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.71.69.236:31440")
                .option("subscribe", "admintome-test")
                .load();
        
        
        Dataset<Tuple3<String,Integer,Timestamp>> tweetDataset = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").map(new MapToTweet(), Encoders.bean(Tweet.class))
                
                .flatMap(new TweetFlatMapFunction(), Encoders.tuple(Encoders.STRING(), Encoders.INT(), Encoders.TIMESTAMP()));
        
        TypedColumn<GenericRowWithSchema,Tuple2<String,Integer>> trending_hashtag = new TrendingHashTagAggregator().toColumn().name("trending_hashtag");
        
        tweetDataset
                .withWatermark("_3", "10 seconds")
                .groupBy(functions.window(col("_3"), "300 seconds", "5 seconds"))
                .agg(trending_hashtag)
                .writeStream()
                .outputMode("append")
                .option("truncate", "false")
                .foreach(new InfluxDBForeachWriter())
                .start();
        
        tweetDataset
                .withWatermark("_3", "10 seconds")
                .groupBy(functions.window(col("_3"), "1 seconds")).count()
                .writeStream()
                .outputMode("append")
                .option("truncate", "false")
                .foreach(new InfluxDBForeachTweetPerSecondWriter())
                .start();
        
        tweetDataset
                .withWatermark("_3", "10 seconds")
                .groupByKey((MapFunction<Tuple3<String,Integer,Timestamp>,String>)stringIntegerTimestampTuple3 -> "count", Encoders.STRING())
                .mapGroupsWithState(new RunningCountMapGroupWithStateFunction(), Encoders.bean(CountInfo.class),
                Encoders.bean(CountUpdate.class),
                GroupStateTimeout.EventTimeTimeout())
                .writeStream()
                .outputMode("update")
                .option("truncate", "false")
                .foreach(new InfluxDBForeachTotalTweetWriter())
                .start();
        
        spark.streams().awaitAnyTermination();
    }
    
    public static class HashTag implements Serializable
    {
        Map<String,Integer> map = new HashMap<>();
        
        public void setMap(Map<String,Integer> map)
        {
            this.map = map;
        }
        
        public void add(String hashTag)
        {
            Integer integer = map.get(hashTag);
            if(integer == null)
            {
                map.put(hashTag, new Integer(1));
            }
            else
            {
                map.put(hashTag, integer + 1);
            }
        }
        
        public Map<String,Integer> getMap()
        {
            return map;
        }
    }
    
    public static class TrendingHashTagAggregator extends Aggregator<GenericRowWithSchema,HashTag,Tuple2<String,Integer>> implements Serializable
    {
        public HashTag zero()
        {
            return new HashTag();
        }
        
        public HashTag reduce(HashTag hashTagbuffer, GenericRowWithSchema hashTag)
        {
            hashTagbuffer.add(hashTag.getString(0));
            return hashTagbuffer;
        }
        
        public HashTag merge(HashTag b1, HashTag b2)
        {
            Map<String,Integer> map1 = b1.getMap();
            Map<String,Integer> map2 = b2.getMap();
            Map<String,Integer> map = new HashMap<>();
            
            for(Map.Entry<String,Integer> entry : map2.entrySet())
            {
                map.put(entry.getKey(), entry.getValue());
            }
            for(Map.Entry<String,Integer> entry : map1.entrySet())
            {
                String hashTag = entry.getKey();
                Integer count1 = entry.getValue();
                Integer count2 = map.get(hashTag);
                if(count2 != null)
                {
                    map.replace(hashTag, count1 + count2);
                }
                else
                {
                    map.put(hashTag, count1);
                }
            }
            HashTag hashTag = new HashTag();
            hashTag.setMap(map);
            return hashTag;
        }
        
        public Tuple2<String,Integer> finish(HashTag reduction)
        {
            Map<String,Integer> map = reduction.getMap();
            String hashTagMax = "";
            Integer countMax = 0;
            for(Map.Entry<String,Integer> entry : map.entrySet())
            {
                String hashTag = entry.getKey();
                Integer count = entry.getValue();
                if(countMax < count)
                {
                    countMax = count;
                    hashTagMax = hashTag;
                }
            }
            Tuple2<String,Integer> tuple2 = new Tuple2<>(hashTagMax, countMax);
            return tuple2;
        }
        
        public Encoder<HashTag> bufferEncoder()
        {
            return Encoders.bean(HashTag.class);
        }
        
        public Encoder<Tuple2<String,Integer>> outputEncoder()
        {
            return Encoders.tuple(Encoders.STRING(), Encoders.INT());
        }
    }
    
    private static class InfluxDBForeachWriter extends ForeachWriter<Row>
    {
        SyncReactiveInfluxDb db;
        SyncReactiveInflux reactiveInflux;
        
        @Override
        public boolean open(long l, long l1)
        {
            try
            {
                ReactiveInfluxConfig config = new JavaReactiveInfluxConfig(new URI("http://10.71.69.236:31948/"));
                reactiveInflux = new JavaSyncReactiveInflux(config, 30000);
                db = reactiveInflux.database("twittergraph");
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            
            return true;
        }
        
        @Override
        public void process(Row row)
        {
            HashMap<String,String> tags = new HashMap<>();
            HashMap<String,Object> fields = new HashMap<>();
            fields.put("hashtag", ((GenericRowWithSchema)row.get(1)).getString(0));
            fields.put("count", ((GenericRowWithSchema)row.get(1)).getInt(1));
            
            Point point = new JavaPoint(
                    new DateTime(((GenericRowWithSchema)row.get(0)).getTimestamp(1).getTime()),
                    "TrendingHashTagSpark",
                    tags,
                    fields
            );
            db.write(point);
        }
        
        @Override
        public void close(Throwable throwable)
        {
            try
            {
                reactiveInflux.close();
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
    }
    
    private static class InfluxDBForeachTweetPerSecondWriter extends ForeachWriter<Row>
    {
        SyncReactiveInfluxDb db;
        SyncReactiveInflux reactiveInflux;
        
        @Override
        public boolean open(long l, long l1)
        {
            try
            {
                ReactiveInfluxConfig config = new JavaReactiveInfluxConfig(new URI("http://10.71.69.236:31948/"));
                reactiveInflux = new JavaSyncReactiveInflux(config, 30000);
                db = reactiveInflux.database("twittergraph");
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            
            return true;
        }
        
        @Override
        public void process(Row row)
        {
            HashMap<String,String> tags = new HashMap<>();
            HashMap<String,Object> fields = new HashMap<>();
            fields.put("count", row.getLong(1));
            
            Point point = new JavaPoint(
                    new DateTime(((GenericRowWithSchema)row.get(0)).getTimestamp(1).getTime()),
                    "TweetPerSecondCountSpark",
                    tags,
                    fields
            );
            db.write(point);
        }
        
        @Override
        public void close(Throwable throwable)
        {
            try
            {
                reactiveInflux.close();
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
    }
    
    private static class InfluxDBForeachTotalTweetWriter extends ForeachWriter<CountUpdate>
    {
        SyncReactiveInfluxDb db;
        SyncReactiveInflux reactiveInflux;
        
        @Override
        public boolean open(long l, long l1)
        {
            try
            {
                ReactiveInfluxConfig config = new JavaReactiveInfluxConfig(new URI("http://10.71.69.236:31948/"));
                reactiveInflux = new JavaSyncReactiveInflux(config, 30000);
                db = reactiveInflux.database("twittergraph");
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            
            return true;
        }
        
        @Override
        public void process(CountUpdate row)
        {
            HashMap<String,String> tags = new HashMap<>();
            HashMap<String,Object> fields = new HashMap<>();
            fields.put("count", row.getCount());
            
            Point point = new JavaPoint(
                    new DateTime(row.getTimestamp().getTime()),
                    "TotalTweetCountSpark",
                    tags,
                    fields
            );
            db.write(point);
        }
        
        @Override
        public void close(Throwable throwable)
        {
            try
            {
                reactiveInflux.close();
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
    }
    
    private static class TweetFlatMapFunction implements FlatMapFunction<Tweet,Tuple3<String,Integer,Timestamp>>
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
    }
    
    private static class MapToTweet implements MapFunction<Row,Tweet>
    {
        @Override
        public Tweet call(Row row) throws Exception
        {
            return new ObjectMapper().readValue(row.getString(1), Tweet.class);
        }
    }
    
    public static class CountInfo implements Serializable
    {
        private long count;
        
        public CountInfo()
        {
        }
        
        public CountInfo(long count)
        {
            this.count = count;
        }
        
        public long getCount()
        {
            return count;
        }
        
        public void setCount(long count)
        {
            this.count = count;
        }
    }
    
    public static class CountUpdate implements Serializable
    {
        private long count;
        private Timestamp timestamp;
        
        public CountUpdate()
        {
        }
    
        public CountUpdate(long count, Timestamp timestamp)
        {
            this.count = count;
            this.timestamp = timestamp;
        }
    
        public long getCount()
        {
            return count;
        }
    
        public void setCount(long count)
        {
            this.count = count;
        }
    
        public Timestamp getTimestamp()
        {
            return timestamp;
        }
    
        public void setTimestamp(Timestamp timestamp)
        {
            this.timestamp = timestamp;
        }
    }
    
    private static class RunningCountMapGroupWithStateFunction implements MapGroupsWithStateFunction<String,Tuple3<String,Integer,Timestamp>,CountInfo,CountUpdate>
    {
        @Override
        public CountUpdate call(String key, Iterator<Tuple3<String,Integer,Timestamp>> values, GroupState<CountInfo> state) throws Exception
        {
            int count = 0;
            while(values.hasNext())
            {
                values.next();
                count++;
            }
            if(state.exists())
            {
                CountUpdate countUpdate = new CountUpdate();
                countUpdate.setCount(count + state.get().getCount());
                countUpdate.setTimestamp(new Timestamp(state.getCurrentWatermarkMs()));
            
                CountInfo countInfo = new CountInfo();
                countInfo.setCount(countUpdate.getCount());
                state.update(countInfo);
                return countUpdate;
            }
            else
            {
                CountUpdate countUpdate = new CountUpdate();
                countUpdate.setCount(count);
                countUpdate.setTimestamp(new Timestamp(state.getCurrentWatermarkMs()));
            
                CountInfo countInfo = new CountInfo();
                countInfo.setCount(countUpdate.getCount());
                state.update(countInfo);
                return countUpdate;
            }
        }
    }
}