package com.amazonaws.services.kinesisanalytics.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.amazonaws.services.rdsdata.AWSRDSData;
import com.amazonaws.services.rdsdata.AWSRDSDataClient;
// import com.amazonaws.services.rdsdata.model.BeginTransactionRequest;
// import com.amazonaws.services.rdsdata.model.BeginTransactionResult;
// import com.amazonaws.services.rdsdata.model.CommitTransactionRequest;
import com.amazonaws.services.rdsdata.model.ExecuteStatementRequest;

import java.util.Properties;
import java.sql.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkDataApiSingle extends RichSinkFunction<Tuple3<String, Long, Timestamp>> {

    private Properties configProps;
    private String transactionId;
    private AWSRDSData rdsData;

    private static final Logger log = LoggerFactory.getLogger(SinkDataApiSingle.class);

    public SinkDataApiSingle(Properties configProps) {
        this.configProps = configProps;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.warn("open");

        this.rdsData = AWSRDSDataClient.builder().build();
/*
        BeginTransactionRequest beginTransactionRequest = new BeginTransactionRequest()
                .withResourceArn(this.configProps.getProperty("RESOURCE_ARN"))
                .withSecretArn(this.configProps.getProperty("SECRET_ARN"))
                .withDatabase(this.configProps.getProperty("DATABASE"));
        BeginTransactionResult beginTransactionResult = this.rdsData.beginTransaction(beginTransactionRequest);
        this.transactionId = beginTransactionResult.getTransactionId();

        log.warn("open transactionId:" + this.transactionId);
*/
        super.open(parameters);

    }

    @Override
    public void invoke(Tuple3<String, Long, Timestamp> value) throws Exception {
        try {
            // log.warn("invoke start");

            // String output = " RAILWAY_CLASS: " + value.f0 + " RAILWAY_CLASS_COUNT: " + value.f1 + " EVENT_TIME: " + value.f2 + "\n";
            String sqlFormat = "INSERT INTO Feature_Collection VALUES ('%s', %s, '%s') " +
                "ON CONFLICT ON CONSTRAINT feature_collection_pkey " +
                "DO UPDATE SET RAILWAY_CLASS_COUNT  = %s, EVENT_TIME = '%s'";

            String sqlString = String.format(sqlFormat, value.f0, value.f1, value.f2.toString(), value.f1, value.f2.toString());

            // String inputString = String.format("%x", value.f1);
            // if (inputString.equals("a")) {
            //     log.warn("invoke sqlString: " + sqlString);
            //     log.warn("f0:" + value.f0 + " f1:" + value.f1 + " f2:" + value.f2.toString());
            // }


            ExecuteStatementRequest executeStatementRequest = new ExecuteStatementRequest()
                    // .withTransactionId(this.transactionId)
                    .withResourceArn(this.configProps.getProperty("RESOURCE_ARN"))
                    .withSecretArn(this.configProps.getProperty("SECRET_ARN"))
                    .withDatabase(this.configProps.getProperty("DATABASE"))
                    .withSql(sqlString);
            this.rdsData.executeStatement(executeStatementRequest);

            // log.warn("invoke done");
        } 
        catch (Exception e) {
            log.warn(e.toString());
            throw e;
        }

    }
/*
    @Override
    public void close() throws Exception {
        CommitTransactionRequest commitTransactionRequest = new CommitTransactionRequest()
                .withTransactionId(this.transactionId)
                .withResourceArn(this.configProps.getProperty("RESOURCE_ARN"))
                .withSecretArn(this.configProps.getProperty("SECRET_ARN"));
        rdsData.commitTransaction(commitTransactionRequest);

        log.warn("close");

        super.close();
    }
*/    
}
