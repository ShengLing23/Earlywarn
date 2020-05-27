package com.sinosoft.earlywarn.Utils;

import com.sinosoft.earlywarn.sendflink.entity.User;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;

public class EarlyWarnProcess extends KeyedProcessFunction<Long,Tuple2<User,Long>,Object> {

    private volatile ValueState<Long> timerState;
    private volatile ValueState<BigDecimal> addMoneyState;
    private static final long N_MINUTE = 10 * 60 * 1000;

    private  ArrayList<Long> flagValue = new ArrayList<>(4);
    private  ArrayList<Boolean> flagBoolean = new ArrayList<>(Collections.nCopies(4,Boolean.valueOf(false)));
     {
        flagValue.add(Long.valueOf(10000));
        flagValue.add(Long.valueOf(50000));
        flagValue.add(Long.valueOf(100000));
        flagValue.add(Long.valueOf(200000));
    }

    @Override
    public void processElement(Tuple2<User,Long> user, Context ctx, Collector<Object> out) throws Exception {
        BigDecimal value = addMoneyState.value();
        if (!Objects.isNull(value)){
            Double money = user.f0.getMoney();
            BigDecimal newValue = value.add(BigDecimal.valueOf(money));
            addMoneyState.update(newValue);
            System.out.println(user.f0.getId()+":========"+money+" =======：累计金额达到"+newValue.longValue());
            long warnValue = isWarn(newValue);
            if (0 != warnValue){
                System.out.println(user.f0.getId() +":===============:消费金额达到了："+warnValue);
            }
        }else {
            addMoneyState.update(BigDecimal.valueOf(0));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
    }

    /**
     * 状态使用之前需要被注册
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<BigDecimal> countMoney = new ValueStateDescriptor<>("countMoney", Types.BIG_DEC);
        ValueStateDescriptor<Long> timer = new ValueStateDescriptor<>("timer", Types.LONG);
        addMoneyState = getRuntimeContext().getState(countMoney);
        timerState = getRuntimeContext().getState(timer);
    }

    private long isWarn(BigDecimal value){
        long warnVal = 0;
        for (int i = 0; i < 4 ; i++) {
            boolean b = flagBoolean.get(i).booleanValue();
            if (!b){
                int result = value.compareTo(BigDecimal.valueOf(flagValue.get(i)));
                if (result == 1 || result == 0){
                    flagBoolean.set(i, Boolean.valueOf(true));
                    warnVal = flagValue.get(i);
                    break;
                }
            }
        }
        return warnVal;
    }

}
