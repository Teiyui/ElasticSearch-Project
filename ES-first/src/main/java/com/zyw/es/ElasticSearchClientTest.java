package com.zyw.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class ElasticSearchClientTest {

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

    @Test
    public void createIndex() throws Exception {
        // 1. 使用client对象创建一个索引库
        client.admin().indices().prepareCreate("hello").get(); // 执行操作
        // 2. 关闭client对象
        client.close();
    }

    @Test
    public void setMappings() throws Exception {
//         1. 创建一个mappings信息
//        XContentBuilder builder = XContentFactory.jsonBuilder()
//                .startObject()
//                   .startObject("article")
//                      .startObject("properties")
//                        .startObject("id")
//                           .field("type", "long")
//                           .field("store", true)
//                        .endObject()
//                        .startObject("title")
//                           .field("type", "long")
//                           .field("store", true)
//                           .field("analyzer", "ik_smart")
//                        .endObject()
//                        .startObject("content")
//                           .field("type", "long")
//                           .field("store", true)
//                           .field("analyzer", "ik_smart")
//                        .endObject()
//                     .endObject()
//                   .endObject()
//                .endObject();
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type","long")
                .field("store", true)
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("store", true)
                .field("analyzer", "ik_smart")
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("store", true)
                .field("analyzer","ik_smart")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        // 使用客户端将mappings信息设置到索引库中
        client.admin().indices().preparePutMapping("hello").setType("article").setSource(builder).get();
        // 关闭客户端
        client.close();
    }

    @Test
    public void testAddDocument() throws Exception {
        // 创建一个client对象
        // 创建一个文档对象
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 12)
                .field("title", "市场监管总局依法对阿里巴巴垄断行为作出行政处罚")
                .field("content", "时建中：促进平台经济在规范中发展")
                .endObject();
        // 把文档对象添加到索引库
        client.prepareIndex("hello", "article", "2").setSource(builder).get();
//        client.prepareIndex()
//                .setId("hello")
//                .setType("article")
//                .setId("1")
//                .setSource(builder)
//                .get();
        // 关闭客户端
        client.close();
    }

    @Test
    public void testAddDocument2() throws Exception {
        // 创建一个Article对象
        Article article = new Article();
        // 设置对象的属性
        article.setId(3);
        article.setTitle("三亚通报“游客称吃海胆蒸蛋没海胆遭威胁”：当季本地海胆汁多肉少");
        article.setContent("航母辽宁舰和美舰碰面后 美国海军官方发布了这张照片");
        // 先把Article对象转换成json格式的字符串
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonDocument = objectMapper.writeValueAsString(article);
        System.out.println(jsonDocument);
        // 使用client对象把文档写入索引库中
        client.prepareIndex("hello", "article", "3")
                .setSource(jsonDocument, XContentType.JSON)
                .get();
        // 关闭客户端
        client.close();
    }
}
