package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.lang.reflect.Method;
import org.apache.commons.lang.exception.ExceptionUtils;
import com.amazonaws.services.kinesisanalytics.StreamJobTumbling;
import com.amazonaws.services.kinesisanalytics.StreamJobSqlSliding;
import com.amazonaws.services.kinesisanalytics.StreamJobSqlHopping;
import com.amazonaws.services.kinesisanalytics.StreamJobSqlTumbling;
import com.amazonaws.services.kinesisanalytics.StreamJobTumblingOffset;

/**
 * Created by lleixu on 11/14/19.
 */
public class Starter {
    private static final Logger log = LoggerFactory.getLogger(Starter.class);

    public static void main(String[] args) throws Exception {
        try {

            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties jobConfigProperties = applicationProperties.get("JobConfigProperties");

            final String[] jobArgs = new String[0];

            switch (jobConfigProperties.getProperty("JOB_CLASS_NAME")) {
                case "StreamJobTumbling":
                    StreamJobTumbling.main(jobArgs);
                case "StreamJobSqlSliding":
                    StreamJobSqlSliding.main(jobArgs);
                case "StreamJobSqlHopping":
                    StreamJobSqlHopping.main(jobArgs);
                case "StreamJobSqlTumbling":
                    StreamJobSqlTumbling.main(jobArgs);
                case "StreamJobTumblingOffset":
                    StreamJobTumblingOffset.main(jobArgs);
            }






/*
            log.warn("kda-flink-demo 1.0.4");

            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties jobConfigProperties = applicationProperties.get("JobConfigProperties");

            final String packageName = Starter.class.getPackage().getName();
            final Class jobClass = Class.forName(packageName + "." + jobConfigProperties.getProperty("JOB_CLASS_NAME"));

            final Method jobMethod = jobClass.getMethod("main", String[].class);
            final Object[] jobArgs = new Object[1];
            jobArgs[0] = new String[0];

            jobMethod.invoke(null, jobArgs[0]);
*/


        }
        catch (Exception e){
            log.error(ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }
}
