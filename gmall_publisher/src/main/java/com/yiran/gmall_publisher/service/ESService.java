package com.yiran.gmall_publisher.service;

import java.util.Map;

/**操作ES相关的数据服务接口
 *      需求：
 *          日活总数
 *          分时日活数
 * @program: gmall_rt0223
 * @description:
 * @author: Mr.Yang
 * @create: 2021-07-07 18:21
 **/
public interface ESService {

    //查询某天的日活数
    public Long getDauTotal(String date);

    //查询每日的分时日活数统计
    public Map<String,Long> getDauHour(String date);
}
