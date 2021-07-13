package com.yiran.gmall_publisher.controller;

import com.yiran.gmall_publisher.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: gmall_rt0223
 * @description: 表示层
 * @author: Mr.Yang
 * @create: 2021-07-07 18:19
 **/
//方法直接返回数据的接口
@RestController
public class PublisherController {

    @Autowired
    private ESService esService;

    /**
     *
     *         访问路径：http://publisher:8070/realtime-total?date=2019-02-01
     *         响应数据：[{"id":"dau","name":"新增日活","value":1200},
     *                     {"id":"new_mid","name":"新增设备","value":233} \]
     *
     * @param dt
     * @return
     */
    @RequestMapping("/realtime-total")
    public Object realtimeTotal(
            @RequestParam("date") String dt
    ){
        Map<String, Object> resMap = new HashMap<String, Object>();
        resMap.put("id","dau");
        resMap.put("name","新增日活");
        Long dauTotal = esService.getDauTotal(dt);
        if (dauTotal!=null){
            resMap.put("value",dauTotal);
        }else {
            resMap.put("value",0L);
        }
        return resMap;
    }

    /**
     *
     *         访问路径：http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
     *         响应：{
     *                 "yesterday":{"11":383,"12":123,"17":88,"19":200 },
     *                 "today":{"12":38,"13":1233,"17":123,"19":688 }
     *                 }
     *   source是 {"1",20L,"2",30L....，"24",50L} 的累计值Map
     * @param dt
     * @return
     */
    @RequestMapping(value = "/realtime-hour")
    public Object realTimeHour(
            @RequestParam("id") String id,
            @RequestParam("date") String dt
    ){

        if ("dau".equals(id)) {
            //存放响应的数据
            Map<String, Map<String, Long>> resMap = new HashMap<>();
            //

            //今日的分时DAU
            Map<String, Long> dauHour = esService.getDauHour(dt);
            //todo:暂时不做非空判断处理了
            resMap.put("today", dauHour);
            //昨日的分时DAU
            Map<String, Long> yesHour = esService.getDauHour(getYesterday(dt));
            resMap.put("yesterday", yesHour);
            return resMap;
        }else
        return null;
    }

    private String getYesterday(String td) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }

}
