package com.orbitz.test;

import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;


public class TestSolrTest extends TestCase{
	
	@Test
	public void testIsPageContent(){
		TestSolr solrTest = new TestSolr();
		String logLine =" /deals/module/orbot/ORB/en_US/75/flights/from-IND-to-PBI/from-Indianapolis-to-West_Palm_Beach ";
		boolean isPageContent = solrTest.isPageContent(logLine);
		System.out.println(isPageContent);
		assertFalse(isPageContent);
		
	}
	
	public void testparseLogLine() throws Exception{
		TestSolr solrTest = new TestSolr();
		String logLine =" /deals/module/orbot/ORB/en_US/75/flights/from-IND-to-PBI/from-Indianapolis-to-West_Palm_Beach ";
		Map<String, Object> map=solrTest.parseLogLine(logLine);
		assertNotNull(map);
		assertEquals(0, map.size());
		
	}
	
	public void testExtractParameter(){
		TestSolr solrTest = new TestSolr();
		String logLine="10.235.165.44 - - [18/Jul/2011:23:59:56 -0500] &quot;GET /deals/pagecontent/EBCH/de_CH//hotel/Frankreich/Cannes/Hotel_Amarante_Cannes.h107830/%7CpageView%3D%7EselectedTab%3Dreviews%7EreviewSortType%3DhighestScore HTTP/1.1&quot; 200 800 0.004 -";
		String parameter = solrTest.extractParameter(logLine);
		assertNotNull(parameter);
		assertTrue(parameter.length()>0);
		assertEquals("pageView=~selectedTab=reviews~reviewSortType=highestScore", parameter);
	}
	
	public void testExtractParameterWithoutParam(){
		TestSolr solrTest = new TestSolr();
		String logLine="10.235.165.44 - - [18/Jul/2011:23:59:56 -0500] &quot;GET /deals/pagecontent/EBCH/de_CH//hotel/Frankreich/Cannes/Hotel_Amarante_Cannes.h107830 HTTP/1.1&quot; 200 800 0.004 -";
		String parameter = solrTest.extractParameter(logLine);
		assertNotNull(parameter);
		assertFalse(parameter.length()>0);
		assertEquals("", parameter);
	}

}
