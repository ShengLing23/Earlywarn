package com.sinosoft.earlywarn.Utils;

import com.alibaba.fastjson.JSON;
import com.sinosoft.earlywarn.sendflink.entity.User;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.io.IOException;

/**
 * 解析字符串
 */
public class KafkaMsgDeserializationSchema implements DeserializationSchema<Tuple2<User,Long>> {


    @Override
    public Tuple2<User, Long> deserialize(byte[] message) throws IOException {
        String value = new String(message, "UTF-8");
        User user = JSON.parseObject(value, User.class);
        return new Tuple2<User,Long>(user,user.getTimestamp());
    }

    @Override
    public boolean isEndOfStream(Tuple2<User, Long> nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Tuple2<User, Long>> getProducedType() {
        return new TupleTypeInfo<Tuple2<User, Long>>(BasicTypeInfo.of(User.class),BasicTypeInfo.LONG_TYPE_INFO);
    }
}
