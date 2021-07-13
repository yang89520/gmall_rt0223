package com.yiran.gmall.realtime.bean

/**
  * Author: Felix
  * Desc: 用户样例类
  */
case class UserInfo(
                     id:String,
                     user_level:String,
                     birthday:String,
                     gender:String,
                     var age_group:String,//年龄段  放到这里不太合理,年龄段会随着时间变化的
                     var gender_name:String) //性别

