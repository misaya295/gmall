package com.cwk.gmall.publisher.service.impl;


import com.cwk.gmall.commom.constant.GmallConstant;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TemplateQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class PublisherServiceImpl implements com.cwk.gmall.publisher.service.PublisherService {


    @Autowired
    JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {


//        String query = "{\n" +
//                "  \"query\":{\n" +
//                "    \"bool\":{\n" +
//                "      \"filter\":{\n" +
//                "        \"term\":{\n" +
//                "          \"logDate\":\"2020-06-06\"\n" +
//                "        }\n" +
//                "      }\n" +
//                "    }\n" +
//                "  }\n" +
//                "}\n";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate", date));
        searchSourceBuilder.query(boolQueryBuilder);

        String s = searchSourceBuilder.toString();
        System.out.println(s);

        Search doc = new Search.Builder(s).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();
        Integer total = 0;

        try {
            SearchResult execute = jestClient.execute(doc);
            total = execute.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }

    @Override
    public Map getDauHourMap(String date) {


        //过滤
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate", date));
        searchSourceBuilder.query(boolQueryBuilder);


        //聚合
        TermsBuilder aggsBuilder = AggregationBuilders.terms("gruopby_logHour").field("logHour").size(24);

        searchSourceBuilder.aggregation(aggsBuilder);





        Search doc = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();

        Map dauHashMap = new HashMap();
        try {
            SearchResult execute = jestClient.execute(doc);
            List<TermsAggregation.Entry> buckets = execute.getAggregations().getTermsAggregation("gruopby_logHour")
                    .getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                String key = bucket.getKey();
                Long count = bucket.getCount();
                dauHashMap.put(key, count);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }


        return dauHashMap;
    }
}
