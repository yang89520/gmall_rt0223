package com.yiran.gmall.realtime.bean

case class OrderInfo(
                      id: Long,  //订单编号
                      province_id: Long, //省份id
                      order_status: String,  //订单状态
                      user_id: Long, //用户id
                      final_total_amount: Double,  //总金额
                      benefit_reduce_amount: Double, //优惠金额
                      original_total_amount: Double, //原价金额
                      feight_fee: Double,  //运费
                      expire_time: String, //失效时间
                      create_time: String, //创建时间
                      operate_time: String,  //操作时间
                      //自己生成
                      var create_date: String, //创建日期
                      var create_hour: String, //创建小时
                      //业务相关 维护一个是否首单的状态值
                      var if_first_order:String, //是否首单
                      //关联省份表
                      var province_name:String,  //地区名
                      var province_area_code:String, //地区编码
                      var province_iso_code:String, //国际地区编码
                      //关联用户表
                      var user_age_group:String, //用户年龄段
                      var user_gender:String //用户性别
                    ) {}
