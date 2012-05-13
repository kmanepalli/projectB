package com.orbitz.test;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class LocationDataGen {

    public static void main(String are[]) { 

        try {
            
            
           
            SolrServer solrLocServer = new CommonsHttpSolrServer("http://egcs02.prod.o.com:8982/location");
            
            FileReader fileReader = new FileReader("/Users/mkchakravarti/Documents/ans_mkt.txt");
            System.out.println("really its here");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            List<String> lines = new ArrayList<String>();
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line);
            }

                    
            SolrQuery solrLocrQuery = new SolrQuery();
            solrLocrQuery.setQueryType("dismax");
            solrLocrQuery.setStart(0);
            solrLocrQuery.setRows(50000);
            solrLocrQuery.setFields("loc.id","loc.locationName,loc.level,loc.city_en_us,loc.city.id");
            QueryResponse queryLocResponse = solrLocServer.query(solrLocrQuery);
            System.out.println(solrLocrQuery);
            SolrDocumentList locList = queryLocResponse.getResults();
            Set<String>  li = new HashSet<String> ();
            for (SolrDocument doc : locList) {
                
                    String loc = (String) doc.getFieldValue("loc.id");
                    String cityId = (String) doc.getFieldValue("loc.city.id");
                    String val = (String) doc.getFieldValue("loc.locationName");
                    String city = (String) doc.getFieldValue("loc.city_en_us");
                    Integer level = (Integer) doc.getFieldValue("loc.level");
                    if(lines.contains(loc)){
                        
                        li.add(cityId + "|" + city);
                  
                    }
                     }
            System.out.println(li.size());
            for (Object object : li) {
                System.out.println(object);
            }
              
            
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
