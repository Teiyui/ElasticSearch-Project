package com.zyw.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

public class SearchIndex {

    private TransportClient client;

    @Before
    public void init() throws Exception {
        // 1. 创建一个Settings对象，相当于一个配置信息，主要配置集群的信息
        Settings settings = Settings.builder()
                .put("cluster.name", "my-elasticsearch")
                .build();
        // 2. 创建一个客户端client对象
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
    }

    private void search(QueryBuilder queryBuilder) throws Exception {
        // 执行查询
        SearchResponse searchResponse = client.prepareSearch("hello").setTypes("article").setQuery(queryBuilder).get();
        // 2. 取查询结果
        SearchHits searchHits = searchResponse.getHits();
        // 3. 取查询结果的总记录数
        System.out.println("查询结果总记录数" + searchHits.getTotalHits());
        // 4. 查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            // 打印文档对象，以json格式输出
            System.out.println(searchHit.getSourceAsString());
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
        }
    }

    @Test
    public void testSearchById() throws Exception {
        // 1. 创建一个查询对象
        IdsQueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1", "2");
        SearchResponse searchResponse = client.prepareSearch("hello").setTypes("article").setQuery(queryBuilder).get();
        // 2. 取查询结果
        SearchHits searchHits = searchResponse.getHits();
        // 3. 取查询结果的总记录数
        System.out.println("查询结果总记录数" + searchHits.getTotalHits());
        // 4. 查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            // 打印文档对象，以json格式输出
            System.out.println(searchHit.getSourceAsString());
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
        }
        // 5. 关闭client对象
        client.close();
    }

    @Test
    public void testQueryByTerm() throws Exception {
        // 创建一个QueryBuilder对象
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 参数1：要搜索的字段
        // 参数2：要搜索的关键词
        TermQueryBuilder queryBuilder = QueryBuilders.termQuery("title", "三亚");
        search(queryBuilder);
    }

    @Test
    public void testQueryStringQuery() throws Exception {
        // 创建一个QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("前端项目").defaultField("title");
        // 执行查询
        search(queryBuilder);
    }

    @Test
    public void testAddDocument3() throws Exception {
        for (int i = 4; i < 104; i++) {
            // 创建一个Article对象
            Article article = new Article();
            // 设置对象的属性
            article.setId(i);
            article.setTitle("如何无痛的为你的前端项目引入多线程" + Integer.toString(i));
            article.setContent("尽管浏览器内核是多线程的，但是负责页面渲染的UI线程总是会在JS引擎线程空闲时" + Integer.toString(i));
            // 先把Article对象转换成json格式的字符串
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonDocument = objectMapper.writeValueAsString(article);
            System.out.println(jsonDocument);
            // 使用client对象把文档写入索引库中
            client.prepareIndex("hello", "article", Integer.toString(i))
                    .setSource(jsonDocument, XContentType.JSON)
                    .get();
        }
        client.close();
    }

    @Test
    public void testSearchByPage() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("前端项目").defaultField("title");
        SearchResponse searchResponse = client.prepareSearch("hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                .setFrom(0)
                .setSize(5)
                .get();
        // 2. 取查询结果
        SearchHits searchHits = searchResponse.getHits();
        // 3. 取查询结果的总记录数
        System.out.println("查询结果总记录数" + searchHits.getTotalHits());
        // 4. 查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            // 打印文档对象，以json格式输出
            System.out.println(searchHit.getSourceAsString());
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
        }
        // 5. 关闭client对象
        client.close();
    }
}
