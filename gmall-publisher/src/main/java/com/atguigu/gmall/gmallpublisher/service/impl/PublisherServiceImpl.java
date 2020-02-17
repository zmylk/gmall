package com.atguigu.gmall.gmallpublisher.service.impl;

import com.atguigu.gmall.constant.GmallConstants;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {


    @Autowired
    JestClient jestClient;

    @Override
    public Integer getDauToal(String date) {
//
//        String query = "{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"logDate\": \"2020-02-03\"\n" +
//                "        }\n" +
//                "      }\n" +
//                "    }\n" +
//                "  }\n" +
//                "}\n";

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        sourceBuilder.query(boolQueryBuilder);
        System.out.println(sourceBuilder.toString());
        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_DAU).addType("_doc").build();
        Integer total = 0 ;
        try {
            SearchResult execute = jestClient.execute(search);
            total = execute.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  total;
    }

    @Override
    public Map getDauHourMap(String date) {
        //过滤
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        sourceBuilder.query(boolQueryBuilder);
        //聚合
        TermsBuilder builder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        sourceBuilder.aggregation(builder);


        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_DAU).addType("_doc").build();
        Map resultMap = new HashMap();
        try {
            SearchResult execute = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = execute.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                resultMap.put(bucket.getKey(),bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        sourceBuilder.query(boolQueryBuilder);

        //过滤
        SumBuilder sumBuilder = AggregationBuilders.sum("aggs_totalAmount").field("totalAmount");
        sourceBuilder.aggregation(sumBuilder);

        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_ORDER).addType("_doc").build();
        Double result = null;
        try {
            SearchResult execute = jestClient.execute(search);
             result = execute.getAggregations().getSumAggregation("aggs_totalAmount").getSum();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Map getOrderAmontHourMap(String date) {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        sourceBuilder.query(boolQueryBuilder);

        //过滤
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);
        SumBuilder sumBuilder = AggregationBuilders.sum("aggs_totalAmount").field("totalAmount");
        termsBuilder.subAggregation(sumBuilder);
        sourceBuilder.aggregation(termsBuilder);

        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_ORDER).addType("_doc").build();
        Map map = new HashMap();
        try {
            SearchResult execute = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = execute.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                Double aggs_totalAmount = bucket.getSumAggregation("aggs_totalAmount").getSum();
                String key = bucket.getKey();
                map.put(key,aggs_totalAmount);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }

    @Override
    public Map getSaleDetailMap(String date, String keyword, int pageNo, int pageSize, String aggsFieldName, int aggsSize) {
        Integer total = 0;
        ArrayList<Map> detailList = new ArrayList<>();
        HashMap<String,Long> aggsMap = new HashMap<>();
        HashMap<Object, Object> saleMap = new HashMap<>();
        //创建查询
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        //全文匹配
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_" + aggsFieldName).field(aggsFieldName).size(aggsSize);
        searchSourceBuilder.aggregation(termsBuilder);

        //分页
        searchSourceBuilder.from((pageNo-1)*pageSize);
        searchSourceBuilder.size(pageSize);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_SALE).addType("_doc").build();
        try {
            SearchResult execute = jestClient.execute(search);
            total = execute.getTotal();
            //明细
            List<SearchResult.Hit<Map, Void>> hits = execute.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source);
            }
            //获取聚合结果
            List<TermsAggregation.Entry> buckets = execute.getAggregations().getTermsAggregation("groupby_" + aggsFieldName).getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                aggsMap.put(bucket.getKey(),bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        saleMap.put("total",total);
        saleMap.put("detail",detailList);
        saleMap.put("aggsMap",aggsMap);
        return saleMap;
    }
}
