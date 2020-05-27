package com.sinosoft.earlywarn.Utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class PrintSink extends RichSinkFunction {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(Object value, Context context) throws Exception {
            System.out.println(value.toString());
    }
}
