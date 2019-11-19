package com.amazonaws.services.kinesisanalytics;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;

import com.amazonaws.services.kinesisanalytics.sink.SinkDataApiTumbling;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;


import java.sql.Timestamp;
import java.util.*;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamJobSqlTumbling {

    private static final Logger log = LoggerFactory.getLogger(StreamJobSqlTumbling.class);

    private static DataStream<ObjectNode> createSourceFromStaticConfig(
            StreamExecutionEnvironment env) throws Exception {

        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties sourceConfigProperties = applicationProperties.get("SourceConfigProperties");

        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, sourceConfigProperties.getProperty("AWS_REGION"));
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, sourceConfigProperties.getProperty("STREAM_INITIAL_POSITION"));

        return env.addSource(new FlinkKinesisConsumer<>(sourceConfigProperties.getProperty("INPUT_STREAM_NAME"),
                new JsonNodeDeserializationSchema(), inputProperties));
    }

    private static SinkDataApiTumbling createSinkFromStaticConfig() throws Exception {
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

        Properties sinkConfigProperties = applicationProperties.get("SinkConfigProperties");

        SinkDataApiTumbling sink = new SinkDataApiTumbling(sinkConfigProperties);
        return sink;
    }

    private static Timestamp getTimestamp(String dateString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try {
            Date parsedDate = dateFormat.parse(dateString);
            Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());

            return timestamp;
        } catch(Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            Date parsedDate = new Date();
            Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
            return timestamp;
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            log.warn("StreamJobSqlTumbling main");

            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties processorConfigProperties = applicationProperties.get("ProcessorConfigProperties");

            String interval = processorConfigProperties.getProperty("INTERVAL_AMOUNT");
            String uom = processorConfigProperties.getProperty("INTERVAL_UOM");
            String checkpointInterval = processorConfigProperties.getProperty("CHECKPOINT_INTERVAL");

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();

            //Set to input data Timestamp indicating the time of the event to process the data sequentially.
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.enableCheckpointing(Long.parseLong(checkpointInterval), CheckpointingMode.EXACTLY_ONCE);

//        log.warn("getCheckpointInterval:" + env.getCheckpointInterval());

            //Leverage JsonNodeDeserializationSchema to convert incoming JSON to generic ObjectNode
            DataStream<ObjectNode> inputStreamObject = createSourceFromStaticConfig(env);

            ObjectMapper jsonParser = new ObjectMapper();

            //Map incomming data from the generic Object Node to a POJO object
            //Set if TimeCharacteristic = "EventTime" to determine how the the Time Attribute rowtime will be determined from the incoming data
            DataStream<Tuple2<String, Timestamp>> inputStream = inputStreamObject
                .map((ObjectNode object) -> {
                    //For debugging input
//                        log.warn(object.toString());

                    JsonNode jsonNode = jsonParser.readValue(object.toString(), JsonNode.class);
                    JsonNode properties = jsonNode.get("properties");

                    Timestamp receivedOn = getTimestamp(properties.get("RECEIVED_ON").asText());

                    // log.warn("receivedOn:" + receivedOn.toString());

                    return new Tuple2<String, Timestamp>(properties.get("N02_001").asText(), receivedOn);
                }).returns(Types.TUPLE(Types.STRING, Types.SQL_TIMESTAMP));


            DataStream<Tuple2<String, Timestamp>> inputStreamWithTime = inputStream
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<String, Timestamp>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Timestamp> element, long previousElementTimestamp) {
                        // log.warn("element.f1:" + element.f1.toString() + " element.f0:" + element.f0);
                        return element.f1.getTime();
                    }

                    @Override
                    public Watermark checkAndGetNextWatermark(Tuple2<String, Timestamp> lastElement, long extractedTimestamp) {
                        return new Watermark(extractedTimestamp);
                    }
                });

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            //Convert from DataStream to Table, replacing replacing incoming field with "Time Attribute" rowtime and aliasing it to orderTime and exchangeRateTime respectively.
            Table inputTable = tableEnv.fromDataStream(inputStreamWithTime, "N02_001, rowtime.rowtime");

            //Register the table for use in SQL queries
            tableEnv.registerTable("Inputs", inputTable);

            //Define a new Dynamic Table as the results of a SQL Query.
            Table resultTable = tableEnv.sqlQuery(""+
                "SELECT " +
                "  CAST (N02_001 AS VARCHAR(10)) AS RAILWAY_CLASS, " +
                "  COUNT(*) RAILWAY_CLASS_COUNT, " +
                "  TUMBLE_START(rowtime, INTERVAL '" + interval + "' " + uom + ") as WINDOW_START, " +
                "  TUMBLE_END(rowtime, INTERVAL '" + interval + "' " + uom + ") as WINDOW_END " +
                " FROM Inputs " +
                " GROUP BY TUMBLE(rowtime, INTERVAL '" + interval + "' " + uom + "), CAST (N02_001 AS VARCHAR(10))"
            );

            //Convert the Dynamic Table to a DataStream
            TupleTypeInfo<Tuple4<String, Long, Timestamp, Timestamp>> tupleType = new TupleTypeInfo<>(
                Types.STRING,
                Types.LONG,
                Types.SQL_TIMESTAMP,
                Types.SQL_TIMESTAMP
            );

            DataStream<Tuple4<String, Long, Timestamp, Timestamp>> resultSet = tableEnv.toAppendStream(resultTable, tupleType);

            resultSet
                .map((Tuple4<String, Long, Timestamp, Timestamp> value) -> {
                    String output = "CLASS: " + value.f0 + " CNT: " + value.f1 + " from: " + value.f2 + " to: " + value.f3 + "\n";
                    log.warn("resultSet output: " + output);

                    return new Tuple4<String, Long, Timestamp, Timestamp>(value.f0, value.f1, value.f2, value.f3);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP))
                .addSink(createSinkFromStaticConfig());

//        log.warn("getCheckpointInterval:" + env.getCheckpointInterval());

            env.execute("AMP GeoJSON Import Count");
        }
        catch (Exception e){
            log.error(ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }
}