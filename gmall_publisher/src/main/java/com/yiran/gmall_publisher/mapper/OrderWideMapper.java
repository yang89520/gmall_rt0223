package com.yiran.gmall_publisher.mapper;

import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface OrderWideMapper {
    //查询当日的实付金额汇总
    BigDecimal selectOrderAmountTotal(@Param("date") String dt);

    //查询分时的当日实付金额汇总
    List<Map<String,BigDecimal>> selectOrderAmountHour(@Param("date") String dt);
}
