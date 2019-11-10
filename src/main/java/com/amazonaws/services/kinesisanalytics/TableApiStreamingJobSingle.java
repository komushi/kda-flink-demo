package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;

import com.amazonaws.services.kinesisanalytics.sink.DataApiSingleSink;

import java.sql.Timestamp;
import java.util.Properties;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableApiStreamingJobSingle {

    private static final String region = "ap-northeast-1";
    private static final String inputStreamName = "amp_geojson";
    private static final String outputDeliveryStreamName = "amp_geojson_sliding_flink";
    private static final String resourceArn = "arn:aws:rds:ap-northeast-1:042083552617:cluster:sls-postgres";
    private static final String secretArn = "arn:aws:secretsmanager:ap-northeast-1:042083552617:secret:sls-postgres-secret-fNbs6H";
    private static final String database = "triad";
    private static final Logger log = LoggerFactory.getLogger(TableApiStreamingJob.class);

    private static DataStream<ObjectNode> createSourceFromStaticConfig(
            StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName,
                new JsonNodeDeserializationSchema(), inputProperties));
    }

    private static FlinkKinesisFirehoseProducer<String> createFirehoseSinkFromStaticConfig() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);

        FlinkKinesisFirehoseProducer<String> sink = new FlinkKinesisFirehoseProducer<>(outputDeliveryStreamName, new SimpleStringSchema(), outputProperties);
        return sink;
    }

    private static DataApiSingleSink createDataApiSinkFromStaticConfig() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty("RESOURCE_ARN", resourceArn);
        outputProperties.setProperty("SECRET_ARN", secretArn);
        outputProperties.setProperty("DATABASE", database);

        DataApiSingleSink sink = new DataApiSingleSink(outputProperties);
        return sink;
    }

    private static Timestamp getTimestamp(String dateString) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
      try {
        Date parsedDate = dateFormat.parse(dateString);
        Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
        log.warn(timestamp.toString() + " vs " + dateString + " vs " + parsedDate.toString());
        return timestamp;
      } catch(Exception e) {
        log.warn(e.toString());
        Date parsedDate = new Date();
        Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
        return timestamp;
      }
    }

    public static void main(String[] args) throws Exception {
      try {
        log.warn("CloudWatch TableApiStreamingJob 1.3.7");

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        //Set to input data Timestamp indicating the time of the event to process the data sequentially.
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //Leverage JsonNodeDeserializationSchema to convert incoming JSON to generic ObjectNode
        DataStream<ObjectNode> inputStreamObject = createSourceFromStaticConfig(env);

        ObjectMapper jsonParser = new ObjectMapper();

        //Map incomming data from the generic Object Node to a POJO object
        //Set if TimeCharacteristic = "EventTime" to determine how the the Time Attribute rowtime will be determined from the incoming data
        DataStream<Tuple2<String, Timestamp>> inputStream = inputStreamObject
          .map((ObjectNode object) -> {
            //For debugging input
            // log.warn(object.toString());

            JsonNode jsonNode = jsonParser.readValue(object.toString(), JsonNode.class);
            JsonNode properties = jsonNode.get("properties");

            Timestamp receivedOn = getTimestamp(properties.get("RECEIVED_ON").asText());

            log.warn("receivedOn:" + receivedOn.toString());
        
            return new Tuple2<String, Timestamp>(properties.get("N02_001").asText(), receivedOn);
          }).returns(Types.TUPLE(Types.STRING, Types.SQL_TIMESTAMP));

        // DataStream<Tuple2<String, Timestamp>> inputStreamWithTime = inputStream
        //   .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Timestamp>>() {
        //     @Override
        //     public long extractAscendingTimestamp(Tuple2<String, Timestamp> element) {
        //       // log.warn("element.f1:" + element.f1.toString() + " element.f0:" + element.f0);
        //       return element.f1.getTime();
        //     }});

        DataStream<Tuple2<String, Timestamp>> inputStreamWithTime = inputStream
          .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<String, Timestamp>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Timestamp> element, long previousElementTimestamp) {
              log.warn("element.f1:" + element.f1.toString() + " element.f0:" + element.f0);
              return element.f1.getTime();
            }

            @Override
            public Watermark checkAndGetNextWatermark(Tuple2<String, Timestamp> lastElement, long extractedTimestamp) {
              return new Watermark(extractedTimestamp);
            }
          });

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        //Convert from DataStream to Table, replacing replacing incoming field with "Time Attribute" rowtime and aliasing it to orderTime and exchangeRateTime respectively.
        Table inputTable = tableEnv.fromDataStream(inputStreamWithTime, "N02_001, RECEIVED_ON, eventtime.rowtime");

        //Register the table for use in SQL queries
        tableEnv.registerTable("Inputs", inputTable);

        //Register UDF for display of Timestamp in output records as a formatted string instead of a number
        // tableEnv.registerFunction("TimestampToString", new TimestampToString());


        //Define a new Dynamic Table as the results of a SQL Query.
        Table resultTable = tableEnv.sqlQuery(""+
          "SELECT " +
          "  CAST (N02_001 AS VARCHAR(10)) AS RAILWAY_CLASS, " +
          "  COUNT(*) OVER (PARTITION BY N02_001 ORDER BY eventtime RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING AND CURRENT ROW) AS RAILWAY_CLASS_COUNT, " +
          "  RECEIVED_ON, " +
          "  eventtime as rowtime " +
          " FROM Inputs "
        );

        //Convert the Dynamic Table to a DataStream
        TupleTypeInfo<Tuple4<String, Long, Timestamp, Timestamp>> tupleType = new TupleTypeInfo<>(
          Types.STRING, 
          Types.LONG, 
          Types.SQL_TIMESTAMP,
          Types.SQL_TIMESTAMP
        );

        DataStream<Tuple4<String, Long, Timestamp, Timestamp>> resultSet = tableEnv.toAppendStream(resultTable, tupleType);

        resultSet.map((Tuple4<String, Long, Timestamp, Timestamp> value) -> {
          // JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
          // String output = mapper.writeValueAsString(jsonNode);

          String output = " RAILWAY_CLASS: " + value.f0 + " RECEIVED_ON: " + value.f2 + " rowtime: " + value.f3 + "\n";

          log.warn("resultSet output: " + output);

          return new Tuple3<String, Long, Timestamp>(value.f0, value.f1, value.f2);
        })
        .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.SQL_TIMESTAMP))
        .addSink(createDataApiSinkFromStaticConfig());


        env.execute("AMP GeoJSON Import Count");
      }
      catch (Exception e){
        log.warn(e.toString());
        throw e;
      }
    }
}