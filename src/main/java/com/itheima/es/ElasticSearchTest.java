package com.itheima.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

public class ElasticSearchTest {


    @Test
    //创建索引
    public void createIndex() throws Exception{
        //创建一个setting对象，相当于是一个配置信息，主要配置集群的名称
        Settings settings = Settings.builder().put("cluster.name","my-elasticsearch").build();
        //创建客户端，并且指定服务器,采用tcp的方式
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        //使用client对象创建一个索引库(索引名字不能大写)
        client.admin().indices().prepareCreate("index_hello")
                .get();//执行
        //关闭client对象
        client.close();

    }

    @Test
    //设置mapping信息
    public void setMap() throws Exception
    {
        //1.创建配置对象setttings
        Settings settings = Settings.builder().put("cluster.name","my-elasticsearch").build();
        //2.创建一个客户端对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        //3.创建mapping数据，可以是json数据，也可以是es客户端提供的XContextBuilder对象
        XContentBuilder builder = XContentFactory.jsonBuilder()
         .startObject()
                .startObject("article")
                    .startObject("properties")
                        .startObject("id")
                            .field("type","long")
                            .field("store",true)
                        .endObject()
                        .startObject("title")
                            .field("type","text")
                            .field("store",true)
                            .field("analyzer","ik_smart")
                        .endObject()
                        .startObject("content")
                            .field("type","text")
                            .field("store",true)
                            .field("analyzer","ik_smart")
                        .endObject()
                    .endObject()
                .endObject()
          .endObject();
        //4.使用客户端向es服务器发送mapping信息
        client.admin().indices()
                //设置要做映射的索引
                .preparePutMapping("index_hello")
                //设置类型索引下面的类型
                .setType("article")
                //设置要做mapping映射的信息，可以是XContentBuiler,也可以是json字符串
                .setSource(builder)
                //执行
                .get();
        //5.关闭客户端
        client.close();
    }


    //添加文档
    @Test
    public void addDocumnet() throws Exception{
        //创建一个settings对象
        Settings settings = Settings.builder().put("cluster.name","my-elasticsearch").build();
        //创建一个client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        //创建一个文档对象，可以是json格式，也可以是XContentBuilder
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("id",7l)
                    .field("title","其实对于破解文件的路径并没有严格要求，只需要后面的配置文件中的对应上就可以")
                    .field("content","点击Activate，就可以完美的激活使用idea了")
                .endObject();
        //使用client对象将文档添加索引库中
        client.prepareIndex()
                //设置索引
                .setIndex("index_hello")
                //设置type
                .setType("article")
                //设置id
                .setId("7")
                //设置文档
                .setSource(builder)
                .get();
        //g关闭client
        client.close();
    }

    //通过json来添加文档
    @Test
    public void addDocument1() throws Exception
    {
        Settings settings = Settings.builder().put("cluster.name","my-elasticsearch").build();
        //创建一个client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        Article article = new Article();
        article.setId(2l);
        article.setTitle("中共中央国务院:保持土地承包关系稳定并长久不变");
        article.setContent("冷空气持续“制冷” 北方大部都将要！下！雪！");
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(article);

        client.prepareIndex("index_hello","article","2")
                .setSource(json, XContentType.JSON)
                .get();
        client.close();


    }


    //根据id进行查询
    @Test
    public void searchById() throws Exception{
        //得到settings对象
        Settings settings = Settings.builder().put("cluster.name","my-elasticsearch").build();
        //创建一个client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        //创建一个查询对象
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1","2");
        //执行查询
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //得到查询结果的总记录数
        System.out.println("查询结果的总记录数:"+searchHits.getTotalHits());
        //查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while(iterator.hasNext())
        {
            SearchHit searchHit = iterator.next();
            //打印文档对象,以json输出
            System.out.println(searchHit.getSourceAsString());
            //取文档的属性
            System.out.println("--------文档的属性");
            Map<String,Object> document = searchHit.getSourceAsMap();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));



        }


    }

    //根据关键词进行查询
    @Test
    public void searchByTerm() throws  Exception{
        //得到settings对象
        Settings settings = Settings.builder().put("cluster.name","my-elasticsearch").build();
        //创建一个client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        //创建一个queryBuilder对象
        //参数1：要搜索的字段，参数2：要搜索的关键字
        QueryBuilder queryBuilder = QueryBuilders.termQuery("title","严格要求");
        //执行查询
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //得到查询结果的总记录数
        System.out.println("查询结果的总记录数:"+searchHits.getTotalHits());
        //查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while(iterator.hasNext())
        {
            SearchHit searchHit = iterator.next();
            //打印文档对象,以json输出
            System.out.println(searchHit.getSourceAsString());
            //取文档的属性
            System.out.println("--------文档的属性");
            Map<String,Object> document = searchHit.getSourceAsMap();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));



        }

    }

    //根据一串文字搜索
    @Test
    public void searchByQueryString() throws Exception{
        //得到settings对象
        Settings settings = Settings.builder().put("cluster.name","my-elasticsearch").build();
        //创建一个client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("我一定严格要求自己")
                //在那个字段上搜索
                .defaultField("title");
        //执行查询
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //得到查询结果的总记录数
        System.out.println("查询结果的总记录数:"+searchHits.getTotalHits());
        //查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while(iterator.hasNext())
        {
            SearchHit searchHit = iterator.next();
            //打印文档对象,以json输出
            System.out.println(searchHit.getSourceAsString());
            //取文档的属性
            System.out.println("--------文档的属性");
            Map<String,Object> document = searchHit.getSourceAsMap();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
        }
    }

    //分页查询
    @Test
    public void searchByPage() throws Exception
    {
        //得到settings对象
        Settings settings = Settings.builder().put("cluster.name","my-elasticsearch").build();
        //创建一个client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("我一定严格要求自己")
                //在那个字段上搜索
                .defaultField("title");
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                //设置页码
                .setFrom(0)
                .setSize(5)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //得到查询结果的总记录数
        System.out.println("查询结果的总记录数:"+searchHits.getTotalHits());
        //查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while(iterator.hasNext())
        {
            SearchHit searchHit = iterator.next();
            //打印文档对象,以json输出
            System.out.println(searchHit.getSourceAsString());
            //取文档的属性
            System.out.println("--------文档的属性");
            Map<String,Object> document = searchHit.getSourceAsMap();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
        }

    }

    //设置结果高亮
    @Test
    public void highLight() throws Exception{
        //得到settings对象
        Settings settings = Settings.builder().put("cluster.name","my-elasticsearch").build();
        //创建一个client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("我一定严格要求自己")
                //在那个字段上搜索
                .defaultField("title");
        //设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //在那个字段上
        highlightBuilder.field("title")
                //前缀
                .preTags("<em>")
                //后缀
                .postTags("</em>");
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                //设置页码
                .setFrom(0)
                .setSize(5)
                .highlighter(highlightBuilder)
                .get();
        //取查询结果
        SearchHits searchHits = searchResponse.getHits();
        //得到查询结果的总记录数
        System.out.println("查询结果的总记录数:"+searchHits.getTotalHits());
        //查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while(iterator.hasNext())
        {
            SearchHit searchHit = iterator.next();
            //打印文档对象,以json输出
            System.out.println(searchHit.getSourceAsString());
            //取文档的属性
            System.out.println("--------文档的属性");
            Map<String,Object> document = searchHit.getSourceAsMap();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
            System.out.println("---------高亮结果");
            //取高亮结果
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            System.out.println(highlightFields);
            HighlightField field = highlightFields.get("title");
            //得到高亮的片段
            Text[] texts = field.getFragments();
            System.out.println(texts[0].toString());
        }

    }


}
