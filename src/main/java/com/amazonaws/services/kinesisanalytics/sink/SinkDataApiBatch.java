package com.amazonaws.services.kinesisanalytics.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;


import com.amazonaws.services.rdsdata.AWSRDSData;
import com.amazonaws.services.rdsdata.AWSRDSDataClient;
import com.amazonaws.services.rdsdata.model.BatchExecuteStatementRequest;
import com.amazonaws.services.rdsdata.model.Field;
import com.amazonaws.services.rdsdata.model.SqlParameter;

import java.util.*;
import java.sql.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkDataApiBatch extends RichSinkFunction<Tuple3<String, Long, Timestamp>>
        implements CheckpointedFunction {

    private Properties configProps;
    private AWSRDSData rdsData;

    private transient ListState<Tuple3<String, Long, Timestamp>> checkpointedState;
    private List<Tuple3<String, Long, Timestamp>> bufferedElements;
    private final int threshold = 10;

    private static final Logger log = LoggerFactory.getLogger(SinkDataApiBatch.class);

    public SinkDataApiBatch(Properties configProps) {
        this.configProps = configProps;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.warn("open");

        this.rdsData = AWSRDSDataClient.builder().build();

        super.open(parameters);

    }

    @Override
    public void invoke(Tuple3<String, Long, Timestamp> value, Context context) throws Exception {
        try {
            // log.warn("invoke start");

            bufferedElements.add(value);

            if (bufferedElements.size() == this.threshold) {
                Collection<List<SqlParameter>> parameterSets = makeParameterSets();

                String sqlString = "INSERT INTO Feature_Collection VALUES (:class, :cnt, to_timestamp(:time, 'yyyy-mm-dd hh24:mi:ss.ms')) " +
                        "ON CONFLICT ON CONSTRAINT feature_collection_pkey " +
                        "DO UPDATE SET RAILWAY_CLASS_COUNT  = :cnt, EVENT_TIME = to_timestamp(:time, 'yyyy-mm-dd hh24:mi:ss.ms')";

                BatchExecuteStatementRequest request = new BatchExecuteStatementRequest()
                    .withResourceArn(this.configProps.getProperty("RESOURCE_ARN"))
                    .withSecretArn(this.configProps.getProperty("SECRET_ARN"))
                    .withDatabase(this.configProps.getProperty("DATABASE"))
                    .withSql(sqlString)
                    .withParameterSets(parameterSets);

                rdsData.batchExecuteStatement(request);

                bufferedElements.clear();
            }

        }
        catch (Exception e) {
            log.warn(e.toString());
            throw e;
        }
    }

    private Collection<List<SqlParameter>> makeParameterSets() {

        List paramList = new ArrayList<>();

        for (Tuple3<String, Long, Timestamp> element: bufferedElements) {
            paramList.add(
                Arrays.asList(
                        new SqlParameter().withName("cnt").withValue(new Field().withLongValue(element.f1)),
                        new SqlParameter().withName("class").withValue(new Field().withStringValue(element.f0)),
                        new SqlParameter().withName("time").withValue(new Field().withStringValue(element.f2.toString()))
                )
            );
        }

        return paramList;
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple3<String, Long, Timestamp> element : bufferedElements) {
            checkpointedState.add(element);
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple3<String, Long, Timestamp>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple3<String, Long, Timestamp>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);



        if (context.isRestored()) {
            for (Tuple3<String, Long, Timestamp> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }

}
