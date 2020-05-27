package com.sinosoft.earlywarn.Utils;

import com.sinosoft.earlywarn.sendflink.entity.User;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;


import javax.annotation.Nullable;

public  class KafkaAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<User,Long>> {

    Long currentMaxTimestamp = 0L;
    final Long maxOutOfOrderness = 10000L;// 最大允许的乱序时间是10sprivate long


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp-maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple2<User, Long> element, long previousElementTimestamp) {
        // 提取时间戳
        long timestamp = element.f1;
        currentMaxTimestamp = Math.max(currentMaxTimestamp,timestamp);
        return timestamp;
    }
}
