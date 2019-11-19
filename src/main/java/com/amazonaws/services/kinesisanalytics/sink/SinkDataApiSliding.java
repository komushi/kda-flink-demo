package com.amazonaws.services.kinesisanalytics.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
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
import java.time.ZonedDateTime;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.exception.ExceptionUtils;

public class SinkDataApiSliding extends RichSinkFunction<Tuple3<String, Long, Timestamp>>
        implements CheckpointedFunction, CheckpointListener {

    private Properties configProps;
    private AWSRDSData rdsData;

//    private transient TupleState tupleState;
    private transient ListState<TupleState> checkpointedState;

    private int count = 0;


    private static final Logger log = LoggerFactory.getLogger(SinkDataApiSliding.class);

    public SinkDataApiSliding(Properties configProps) {
        this.configProps = configProps;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        log.warn("open");

        super.open(parameters);

        this.rdsData = AWSRDSDataClient.builder().build();

    }

    @Override
    public void invoke(Tuple3<String, Long, Timestamp> value, Context context) throws Exception {
        try {
//            log.warn("invoke start: " + ZonedDateTime.now().toString() + "f0:" + value.f0 + " f1:" + value.f1.toString());

//            count++;
//            log.warn("count:" + count);

            initializeTupleState();

            TupleState checkpointedTupleState = checkpointedState.get().iterator().next();

            synchronized (checkpointedTupleState.pendingTuples) {
                checkpointedTupleState.pendingTuples.add(value);

                if (checkpointedTupleState.pendingTuples.size() >= Integer.valueOf(this.configProps.getProperty("THRESHOLD"))) {
                    batchExecute(checkpointedTupleState.pendingTuples);
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
//            log.warn("snapshotState:" + ZonedDateTime.now().toString());

            long checkpointId = context.getCheckpointId();

            initializeTupleState();

            TupleState tupleState = checkpointedState.get().iterator().next();

            synchronized(tupleState.pendingTuplesPerCheckpoint) {
                List<Tuple3<String, Long, Timestamp>> checkpointedTupleList = tupleState.pendingTuplesPerCheckpoint.get(checkpointId);

                if (checkpointedTupleList == null) {
                    checkpointedTupleList = new ArrayList<>();
                    tupleState.pendingTuplesPerCheckpoint.put(checkpointId, checkpointedTupleList);
                }

                checkpointedTupleList.addAll(tupleState.pendingTuples);
            }


            synchronized (tupleState.pendingTuples) {
                tupleState.pendingTuples.clear();
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

//            log.warn("initializeState:" + ZonedDateTime.now().toString());

            ListStateDescriptor<TupleState> descriptor =
                    new ListStateDescriptor<>(
                            "pending-tuples",
                            TypeInformation.of(new TypeHint<TupleState>() {}));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                int cnt = 0;
                for (TupleState element : checkpointedState.get()) {
                    cnt++;
                    log.warn(cnt + " TupleState(s) with " + element.toString());
                }
            }
            
        }
        catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw e;
        }

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        try {
//            log.warn("notifyCheckpointComplete:" + ZonedDateTime.now().toString());

            TupleState tupleState = checkpointedState.get().iterator().next();

            synchronized(tupleState.pendingTuplesPerCheckpoint) {
                Iterator<Map.Entry<Long, List<Tuple3<String, Long, Timestamp>>>> pendingCheckpointsIt =
                        tupleState.pendingTuplesPerCheckpoint.entrySet().iterator();

                while (pendingCheckpointsIt.hasNext()) {
                    Map.Entry<Long, List<Tuple3<String, Long, Timestamp>>> entry = pendingCheckpointsIt.next();
                    Long pastCheckpointId = entry.getKey();
                    List<Tuple3<String, Long, Timestamp>> checkpointedTuples = entry.getValue();

                    if (pastCheckpointId <= checkpointId) {

                        synchronized (checkpointedTuples) {
                            batchExecute(checkpointedTuples);
                        }

                        pendingCheckpointsIt.remove();
                    }
                }
            }

        }
        catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    private void initializeTupleState() throws Exception {
        int cnt = 0;

        for (Object obj : checkpointedState.get()) {
            cnt++;
        }


        if (cnt == 0) {
            TupleState checkpointedTupleState = new TupleState();
            checkpointedState.add(checkpointedTupleState);
        }
    }

   private Collection<List<SqlParameter>> makeParameterSets(List<Tuple3<String, Long, Timestamp>> pendingTuples) {

       List paramList = new ArrayList<>();

       for (Tuple3<String, Long, Timestamp> element: pendingTuples) {
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

    private synchronized void batchExecute(List<Tuple3<String, Long, Timestamp>> tuples) {


        if (tuples.size() > 0) {

            Collection<List<SqlParameter>> parameterSets = makeParameterSets(tuples);

            String sqlString = "INSERT INTO Sliding VALUES (:class, :cnt, to_timestamp(:time, 'yyyy-mm-dd hh24:mi:ss.ms')) " +
                "ON CONFLICT ON CONSTRAINT sliding_pkey " +
                "DO UPDATE SET RAILWAY_CLASS_COUNT  = :cnt, EVENT_TIME = to_timestamp(:time, 'yyyy-mm-dd hh24:mi:ss.ms')";


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

    static final class TupleState implements Serializable {

        /**
         * Pending files that accumulated since the last checkpoint.
         */
        public List<Tuple3<String, Long, Timestamp>> pendingTuples = new ArrayList<>();

        /**
         * When doing a checkpoint we move the pending files since the last checkpoint to this map
         * with the id of the checkpoint. When we get the checkpoint-complete notification we move
         * pending files of completed checkpoints to their final location.
         */
        public final Map<Long, List<Tuple3<String, Long, Timestamp>>> pendingTuplesPerCheckpoint = new HashMap<>();

        @Override
        public String toString() {
            return "pendingTuples.size():" + pendingTuples.size() + " pendingTuplesPerCheckpoint.size():" + pendingTuplesPerCheckpoint.size();
        }
    }
}
