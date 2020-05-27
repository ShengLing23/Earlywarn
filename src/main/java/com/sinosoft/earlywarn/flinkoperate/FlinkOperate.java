package com.sinosoft.earlywarn.flinkoperate;


import com.sinosoft.earlywarn.Utils.EarlyWarnProcess;
import com.sinosoft.earlywarn.Utils.KafkaAssignerWithPeriodicWatermarks;
import com.sinosoft.earlywarn.Utils.KafkaMsgDeserializationSchema;
import com.sinosoft.earlywarn.sendflink.entity.User;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.sink.AlertSink;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Properties;

public class FlinkOperate {

    private Properties kafkaProp;

    private void initKafakPropertiesInfo(){
        kafkaProp = new Properties();
        kafkaProp.put("bootstrap.servers","localhost:9092");
        kafkaProp.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProp.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProp.put("group.id","flink-group");
    }

    private void flinkOperate() throws Exception {
        final  StreamExecutionEnvironment env = getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple2<User, Long>> tuple2DataStreamSource = addSource(env);
        //windowsOperate(tuple2DataStreamSource);
        processOperate(tuple2DataStreamSource);
        env.execute("flink-operate-data");
    }

    //=================== Operate methods =====================
    private void windowsOperate(DataStream<Tuple2<User, Long>> tuple2DataStreamSource) {
        SingleOutputStreamOperator<String> apply = tuple2DataStreamSource
                .keyBy(d -> d.f0.getId())
                .timeWindow(Time.minutes(2), Time.seconds(10))
                .apply(new WindowFunction<Tuple2<User, Long>, String, Long, TimeWindow>() {
                    /**
                     *
                     * @param aLong
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<Tuple2<User, Long>> input, Collector<String> out) throws Exception {
                        Iterator<Tuple2<User, Long>> iterator = input.iterator();
                        double money = 0;
                        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                        while (iterator.hasNext()) {
                            Tuple2<User, Long> next = iterator.next();
                            money += next.f0.getMoney();
                        }
                        StringBuilder sb = new StringBuilder();
                        if (money > 500) {
                            sb.append("money-gt-500 ");
                            sb.append(" User id is ").append(aLong.toString());
                            sb.append(" Total money is ").append(money);
                            String start = dtf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(window.getStart()), ZoneId.systemDefault()));
                            String end = dtf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(window.getEnd()), ZoneId.systemDefault()));
                            sb.append(" time is between ").append(start)
                                    .append(" and ").append(end);
                            out.collect(sb.toString());
                        } /*else {
                            sb.append("money-lt-500");
                            sb.append(" User id is ").append(aLong.toString());
                            sb.append(" Total money is ").append(money);
                            String start = dtf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(window.getStart()), ZoneId.systemDefault()));
                            String end = dtf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(window.getEnd()), ZoneId.systemDefault()));
                            sb.append(" time is between ").append(start)
                                    .append(" and ").append(end);
                            System.out.println(sb.toString());
                        }*/
                    }
                });
        // 打印到控制台
        apply.print();
    }

    private void processOperate(DataStream<Tuple2<User, Long>> tuple2DataStreamSource){
        SingleOutputStreamOperator<Object> warn_operate = tuple2DataStreamSource
                .keyBy(d -> d.f0.getId())
                .process(new EarlyWarnProcess())
                .name("warn operate");

    }

    //================== Utils Methods ========================
    private DataStreamSink<String> addSink(DataStreamSource<String> env) {
         return env.addSink(new FlinkKafkaProducer010<>(
                "localhost:9092",
                "flink-sink",
                new SimpleStringSchema()
                )).name("flink-kafka-sink");
    }

    private DataStream<Tuple2<User, Long>> addSource(StreamExecutionEnvironment env) {
        SingleOutputStreamOperator<Tuple2<User, Long>> kafkaTopic = env.addSource(new FlinkKafkaConsumer010<>(
                "flinkTest",
                new KafkaMsgDeserializationSchema(),
                kafkaProp))
                .assignTimestampsAndWatermarks(new KafkaAssignerWithPeriodicWatermarks());

        return kafkaTopic;
    }

    private StreamExecutionEnvironment getExecutionEnvironment() {
        return StreamExecutionEnvironment
                    .getExecutionEnvironment();
    }

    public static void main(String[] args) {
        FlinkOperate flinkOperate = new FlinkOperate();
        flinkOperate.initKafakPropertiesInfo();
        try {
            flinkOperate.flinkOperate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
