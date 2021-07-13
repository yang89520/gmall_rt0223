package com.yiran.gmall_publisher.service.impl;

import com.yiran.gmall_publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @program: gmall_rt0223
 * @description:
 * @author: Mr.Yang
 *
 * \@Component //初代版本spring一下三个注解都是这个管理
 * \@Controller //表示层
 * \@Service //业务层
 * \@Repository //持久层
 *
 **/

@Service
public class ESServiceImpl implements ESService {

    // 将ES的客户端操作对象注入到Service中
    // springboot 1.5.10版本不能使用，2.5.10版本的可以导入
    @Autowired
    JestClient jestClient;


    /**
     * 查询 Dau 总数
     * 
     *     GET /gmall0523_dau_info_2020-10-24-query/_search
     *     {
     *       "query": {
     *         "match_all": {}
     *       }
     *     }
     *
     * @param date
     * @return
     */
    @Override
    public Long getDauTotal(String date) {
        //建造者模式 链式调用方法 返回的还是 SearchSourceBuilder 对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有
        sourceBuilder.query(new MatchAllQueryBuilder());
        //转为String
        String query = sourceBuilder.toString();
        //表名index
        String indexName = "gmall0223_dau_info_" + date + "-query";
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();
        //查询的总数用于返回 每日总共的Dau人数
        Long searchTotal = 0L;
        try {//execute需要处理异常
            SearchResult searchResult = jestClient.execute(search);
            searchTotal = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES DauTotal 失败");
        }
        return searchTotal;
    }


    /**
     * 查询每日每小时的Dau统计值
     *     GET /gmall0523_dau_info_2020-10-24-query/_search
     *     {
     *       "aggs": {
     *         "groupBy_hr": { //聚合函数名
     *           "terms": {
     *             "field": "hr", //岑组字段
     *             "size": 24 //展示24条搜索内容 一日内分组最多也就是24
     *           }
     *         }
     *       }
     *     }
     * @param date
     * @return Map<String, Long>
     */
    @Override
    public Map<String, Long> getDauHour(String date) {
        /**
        Map<String, Long> hourMap = new HashMap<>();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("groupBy_hr", ValueType.LONG)
                .field("hr").size(24);
        // public SearchSourceBuilder aggregation(AggregationBuilder aggregation)
        sourceBuilder.aggregation(termsAggregationBuilder);

        //index
        String idnexName = "gmall0223_dau_info" + date + "-query";
        //query
        String query = sourceBuilder.toString();
        //Search 对象
        Search search = new Search.Builder(query)
                .addIndex(idnexName)
                .build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //拿到 TermsAggregation.Entry terms对象
            TermsAggregation termsAggs = searchResult.getAggregations().getTermsAggregation("groupBy_hr");
            //非空进行取数逻辑
            if (termsAggs!=null){
                List<TermsAggregation.Entry> buckets = termsAggs.getBuckets();
                buckets.forEach(bucket->{
                    hourMap.put(bucket.getKey(),bucket.getCount());
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES DauHour 失败");
        }
        return hourMap;
    }
         */

        Map<String, Long> hourMap = new HashMap<>();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder =
                new TermsAggregationBuilder("groupBy_hr", ValueType.LONG).field("hr").size(24);

        sourceBuilder.aggregation(termsAggregationBuilder);

        String indexName = "gmall0223_dau_info_"+ date +"-query";
        String query = sourceBuilder.toString();
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            /**
             *   "aggregations" : {
             *     "groupBy_hr" : {
             *       "doc_count_error_upper_bound" : 0,
             *       "sum_other_doc_count" : 0,
             *       "buckets" : [
             *         {
             *           "key" : "17",
             *           "doc_count" : 50
             *         }
             *       ]
             *     }
             *   }
             *   思路： aggregations--->
             */
            TermsAggregation termsAgg = result.getAggregations().getTermsAggregation("groupBy_hr");
            if(termsAgg!=null){
                List<TermsAggregation.Entry> buckets = termsAgg.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    hourMap.put(bucket.getKey(),bucket.getCount());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }
        return hourMap;
    }
}
