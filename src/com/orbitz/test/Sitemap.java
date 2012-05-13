package com.orbitz.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.json.JSONObject;

import com.redfin.sitemapgenerator.ChangeFreq;
import com.redfin.sitemapgenerator.WebSitemapGenerator;
import com.redfin.sitemapgenerator.WebSitemapUrl;

public class Sitemap {

    public List <String> getIds(int i) throws Exception {
        List <String> ids = new ArrayList <String>();

        SolrServer hotServer = new CommonsHttpSolrServer("http://indexedsearch.prod.o.com:8980/hotelLP");
        SolrQuery solrLocrQuery = new SolrQuery();

        solrLocrQuery.setQueryType("standard");
        solrLocrQuery.addFilterQuery("pos:ORB");
        solrLocrQuery.setRows(10000);
        solrLocrQuery.setStart(i*10000);
        solrLocrQuery.setFields("hot.id");
        solrLocrQuery.setSortField("hot.city.sort", ORDER.asc);
        QueryResponse queryLocResponse = hotServer.query(solrLocrQuery);
        SolrDocumentList locList = queryLocResponse.getResults();

        for (SolrDocument doc : locList) {
            try {
                ids.add(doc.get("hot.id").toString());
            } catch (Exception e) {

            }
        }
        return ids;
    }

    public static void main(String p[]) throws Exception {

        Sitemap map = new Sitemap();
        for(int i=8;i<11;i++){
        List <String> ids = map.getIds(i);
        System.out.println("fetched Ids");
        List <String> canons = new ArrayList <String>();
        for (String id : ids) {
            String url = "http://egcmsref01.prod.o.com:10000/deals/module/hotelsCanonical/ORB/en_US/43/"+id;
            try {
                URL u = new URL(url);
                String line = null;
                HttpURLConnection connection = (HttpURLConnection) u.openConnection();
                connection.setRequestMethod("GET");
                connection.setDoOutput(true);
                connection.setReadTimeout(3000);

                connection.connect();
                BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder sb = new StringBuilder();

                while ((line = rd.readLine()) != null) {
                    sb.append(line + '\n');
                }

                JSONObject obj = new JSONObject(sb.toString());
                JSONObject o = obj.getJSONObject("onegToCanonical");
                String canUrl = o.getString(id);
                canUrl = canUrl.replace("cmsref.", "");
                
                canons.add(canUrl);
                connection.disconnect();
            } catch (MalformedURLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        System.out.println("fetched Urls");
        WebSitemapGenerator web = new WebSitemapGenerator("http://www.orbitz.com",new File("/Users/mkchakravarti/Documents/project/sitemap/"+i+"/")) ;
       // SitemapIndexGenerator sig = new SitemapIndexGenerator("http://www.orbitz.com",new File("/tmp/sitemap_1.xml"));
        
        try {
            for(String canon:canons){
                //System.out.println("canon"+canon);
                if(StringUtils.isNotEmpty(canon) && !canon.contains("hotels")){
                    WebSitemapUrl.Options op =new WebSitemapUrl.Options(canon);
                    op.changeFreq(ChangeFreq.MONTHLY);
                    WebSitemapUrl u = new WebSitemapUrl(op) ;
                    
                    web.addUrl(u);
                }
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        web.write();
    }
    }
}
