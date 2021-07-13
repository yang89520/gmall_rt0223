package com.yiran.gmall_publisher.service.impl;

import com.yiran.gmall_publisher.mapper.OrderWideMapper;
import com.yiran.gmall_publisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: gmall_rt0223
 * @description: 业务层的抽象接口实现类：
 *                  定义Mapper
 *                  实现抽象方法
 *                      因为mybatis是orm的对象关系映射 OrderWideMapper.xml里的namespace到mapper的数据查询接口 ，
 *                      所以这里调用接口的查询方法即可返回数据库的操作结果
 * @author: Mr.Yang
 * @create: 2021-07-10 11:35
 **/

public class ClickHouseServiceImpl implements ClickHouseService {
    @Autowired
    OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getOrderAmountTocal(String date) {
        return orderWideMapper.selectOrderAmountTotal(date);
    }

    /**
     * 接口返回：List(Map(k,v),Map(k,v)...)
     * return接收结果：Map(k1->v1,k2_>v2,...)
     * @param date
     * @return
     */
    @Override
    public Map<String, BigDecimal> getOrderAmountHour(String date) {
        Map<String, BigDecimal> resMap = new HashMap<>();
        List<Map<String, BigDecimal>> resListWithMap =  orderWideMapper.selectOrderAmountHour(date);
        for (Map<String, BigDecimal> res : resListWithMap) {
            resMap.put(String.format("%02d",res.get("hr")),(BigDecimal) res.get("am"));
        }
        return resMap;
    }
}
