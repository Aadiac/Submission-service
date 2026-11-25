package example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

public class SubmissionAggregationJob {
  static final ObjectMapper MAPPER = new ObjectMapper();

  public static class Event {
    public long submissionId; public String formId; public long eventTime;
    public Event() {}
    Event(long id,String form,long ts){submissionId=id;formId=form;eventTime=ts;}
  }
  public static class Count { public String formId; public long hourStart; public long count;
    public Count(){} Count(String f,long h,long c){formId=f;hourStart=h;count=c;} }

  public static class ParseEvent implements org.apache.flink.api.common.functions.MapFunction<String,Event> {
    public Event map(String value) throws Exception {
      JsonNode n=MAPPER.readTree(value);
      return new Event(n.get("submission_id").asLong(),n.get("form_id").asText(),
          Instant.parse(n.get("created_at").asText()).toEpochMilli());
    }
  }

  // The outbox publisher can resend after a crash between Kafka publish and DB acknowledgement.
  // Key by submission ID and remember processed IDs in checkpointed keyed state.
  public static class Deduplicate extends KeyedProcessFunction<Long,Event,Event> {
    private transient ValueState<Boolean> seen;
    public void open(org.apache.flink.configuration.Configuration c) {
      ValueStateDescriptor<Boolean> d=new ValueStateDescriptor<>("seen-submission",Boolean.class);
      seen=getRuntimeContext().getState(d);
    }
    public void processElement(Event e,Context ctx,Collector<Event> out) throws Exception {
      if(seen.value()==null) { seen.update(true); out.collect(e); }
    }
  }

  public static class Sum implements AggregateFunction<Event,Long,Long> {
    public Long createAccumulator(){return 0L;}
    public Long add(Event e,Long acc){return acc+1;}
    public Long getResult(Long acc){return acc;}
    public Long merge(Long a,Long b){return a+b;}
  }
  public static class AddWindow extends ProcessWindowFunction<Long,Count,String,TimeWindow> {
    public void process(String form,Context ctx,Iterable<Long> values,Collector<Count> out) {
      out.collect(new Count(form,ctx.window().getStart(),values.iterator().next()));
    }
  }

  public static void main(String[] args) throws Exception {
    String brokers=env("KAFKA_BROKERS","kafka:29092");
    String topic=env("KAFKA_TOPIC","submission-events");
    String group=env("KAFKA_GROUP","flink-submission-aggregation");
    String jdbc=env("JDBC_URL","jdbc:postgresql://postgres:5432/submissions");
    StreamExecutionEnvironment executionEnv=StreamExecutionEnvironment.getExecutionEnvironment();
    executionEnv.enableCheckpointing(10000);
    executionEnv.getCheckpointConfig().setCheckpointTimeout(60000);
    executionEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
    executionEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    KafkaSource<String> source=KafkaSource.<String>builder()
      .setBootstrapServers(brokers).setTopics(topic).setGroupId(group)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema()).build();

    DataStream<Event> events=executionEnv.fromSource(source,
      WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((value,ts)->{
          try { return Instant.parse(MAPPER.readTree(value).get("created_at").asText()).toEpochMilli(); }
          catch(Exception ex){ return System.currentTimeMillis(); }
        }),"kafka-submissions")
      .map(new ParseEvent()).returns(TypeInformation.of(Event.class));

    DataStream<Event> unique=events.keyBy(e->e.submissionId).process(new Deduplicate())
      .returns(TypeInformation.of(Event.class));

    DataStream<Count> counts=unique.assignTimestampsAndWatermarks(
        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
          .withTimestampAssigner((e,ts)->e.eventTime))
      .keyBy(e->e.formId)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .aggregate(new Sum(),new AddWindow());

    counts.addSink(JdbcSink.sink(
      "INSERT INTO hourly_aggregations(form_id,hour_start,submission_count) VALUES(?,?,?) " +
      "ON CONFLICT(form_id,hour_start) DO UPDATE SET submission_count=EXCLUDED.submission_count",
      (ps,c)->{ps.setString(1,c.formId);ps.setTimestamp(2,new Timestamp(c.hourStart));ps.setLong(3,c.count);},
      JdbcExecutionOptions.builder().withBatchSize(100).withBatchIntervalMs(1000).withMaxRetries(3).build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(jdbc)
        .withDriverName("org.postgresql.Driver").withUsername(env("DB_USER","app"))
        .withPassword(env("DB_PASSWORD","app")).build()
    ));
    executionEnv.execute("submission-hourly-aggregation");
  }
  static String env(String k,String d){String v=System.getenv(k);return v==null?d:v;}
}
