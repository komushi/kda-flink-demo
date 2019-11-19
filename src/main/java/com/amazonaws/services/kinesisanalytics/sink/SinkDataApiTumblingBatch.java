package com.amazonaws.services.kinesisanalytics.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;


import com.amazonaws.services.rdsdata.AWSRDSData;
import com.amazonaws.services.rdsdata.AWSRDSDataClient;
import com.amazonaws.services.rdsdata.model.BatchExecuteStatementRequest;
import com.amazonaws.services.rdsdata.model.Field;
import com.amazonaws.services.rdsdata.model.SqlParameter;

import java.util.*;
import java.sql.Timestamp;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.exception.ExceptionUtils;

public class SinkDataApiTumblingBatch extends RichSinkFunction<Tuple4<String, Long, Timestamp, Timestamp>>
        implements CheckpointedFunction {

    private Properties configProps;
    private AWSRDSData rdsData;

//    private transient TupleState tupleState;
    private transient ListState<Tuple4<String, Long, Timestamp, Timestamp>> checkpointedState;
    public List<Tuple4<String, Long, Timestamp, Timestamp>> pendingTuples;


    private static final Logger log = LoggerFactory.getLogger(SinkDataApiTumblingBatch.class);

    public SinkDataApiTumblingBatch(Properties configProps) {
        this.configProps = configProps;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        log.warn("open");

        super.open(parameters);

        this.rdsData = AWSRDSDataClient.builder().build();
        this.pendingTuples = new ArrayList<>();

    }

    @Override
    public void invoke(Tuple4<String, Long, Timestamp, Timestamp> value, Context context) throws Exception {
        try {
           log.warn("invoke start: " +  "f0:" + value.f0 + " f1:" + value.f1 + " f2:" + value.f2);

            synchronized (this.pendingTuples) {
                this.pendingTuples.add(value);

                if (this.pendingTuples.size() >= Integer.valueOf(this.configProps.getProperty("THRESHOLD"))) {
                    batchExecute(this.pendingTuples);
                    this.pendingTuples.clear();
                }
            }

        }
        catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        try {
//            log.warn("snapshotState!");

            synchronized(checkpointedState) {
                checkpointedState.clear();
                for (Tuple4<String, Long, Timestamp, Timestamp> element : this.pendingTuples) {
                    checkpointedState.add(element);
                }
            }

        }
        catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw e;
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        try {

//            log.warn("initializeState!");

            ListStateDescriptor<Tuple4<String, Long, Timestamp, Timestamp>> descriptor =
                    new ListStateDescriptor<>(
                            "pending-tuples",
                            TypeInformation.of(new TypeHint<Tuple4<String, Long, Timestamp, Timestamp>>() {}));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                int cnt = 0;
                for (Tuple4<String, Long, Timestamp, Timestamp> element : checkpointedState.get()) {
                    this.pendingTuples.add(element);
                }
            }

        }
        catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw e;
        }

    }

    private Collection<List<SqlParameter>> makeParameterSets(List<Tuple4<String, Long, Timestamp, Timestamp>> pendingTuples) {

        List paramList = new ArrayList<>();

        for (Tuple4<String, Long, Timestamp, Timestamp> element: pendingTuples) {
            paramList.add(
                    Arrays.asList(
                            new SqlParameter().withName("cnt").withValue(new Field().withLongValue(element.f1)),
                            new SqlParameter().withName("class").withValue(new Field().withStringValue(element.f0)),
                            new SqlParameter().withName("start").withValue(new Field().withStringValue(element.f2.toString())),
                            new SqlParameter().withName("end").withValue(new Field().withStringValue(element.f3.toString()))
                    )
            );
        }

        return paramList;
    }


    private synchronized void batchExecute(List<Tuple4<String, Long, Timestamp, Timestamp>> tuples) {


        if (tuples.size() > 0) {

            Collection<List<SqlParameter>> parameterSets = makeParameterSets(tuples);

            String sqlString = "INSERT INTO Tumbling VALUES (:class, :cnt, to_timestamp(:start, 'yyyy-mm-dd hh24:mi:ss.ms'), to_timestamp(:end, 'yyyy-mm-dd hh24:mi:ss.ms')) " +
                    "ON CONFLICT ON CONSTRAINT tumbling_pkey " +
                    "DO UPDATE SET RAILWAY_CLASS_COUNT  = :cnt";

            BatchExecuteStatementRequest request = new BatchExecuteStatementRequest()
                    .withResourceArn(this.configProps.getProperty("RESOURCE_ARN"))
                    .withSecretArn(this.configProps.getProperty("SECRET_ARN"))
                    .withDatabase(this.configProps.getProperty("DATABASE"))
                    .withSql(sqlString)
                    .withParameterSets(parameterSets);

            log.warn(tuples.size() + " tuples: " + request.toString());

            rdsData.batchExecuteStatement(request);

            tuples.clear();
        }
    }

}
