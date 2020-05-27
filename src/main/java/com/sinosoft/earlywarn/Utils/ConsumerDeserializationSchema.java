package com.sinosoft.earlywarn.Utils;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class ConsumerDeserializationSchema<T>  implements DeserializationSchema<T> {

    private Class<T> clazz;

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        String value = new String(bytes);
        return JSON.parseObject(value,clazz);
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(clazz);
    }
}
