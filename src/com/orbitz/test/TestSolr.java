package com.orbitz.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class TestSolr {

	private static final String ACCESS_LOG_FILE_PATH = "/Users/mkchakravarti/Documents/project/deals/access/";
	private static SolrServer solrServer;

	public static void main(String args[]) throws Exception {
		TestSolr test = new TestSolr();
		test.accessDataLoader();
	}

	public void accessDataLoader() throws Exception {
		solrServer = createSolrServer();
		accessLogParser();
		optimize();
	}

	public void accessLogParser() throws Exception {
		File f = new File(ACCESS_LOG_FILE_PATH);
		if (f.isDirectory()) {
			File[] files = f.listFiles();
			for (File file : files) {
				List<SolrInputDocument> docList = parseFile(file);
				addSolr(docList);
			}
		}

	}

	public List<SolrInputDocument> parseFile(File file) throws Exception {

		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		List<SolrInputDocument> docList = new ArrayList<SolrInputDocument>();
		while (br.readLine() != null) {
			String logLine = br.readLine();

			Map<String, Object> urlMap = parseLogLine(logLine);
			SolrInputDocument inputDocument = createSolrInputDocument(urlMap);
			if (!inputDocument.isEmpty()) {
				docList.add(inputDocument);
			}

		}
		return docList;

	}

	protected Map<String, Object> parseLogLine(String logLine)
			throws ParseException {
		Map<String, Object> logParseMap = new HashMap<String, Object>();
		if (isPageContent(logLine) && !logLine.trim().isEmpty()) {
			logLine = logLine.trim();
			String url = extractUrl(logLine);
			String time = extractTime(logLine);
			String statusLatency = extractStatusLatency(logLine);
			String parameters = extractParameter(logLine);
			Map<String, String> urlMap = parseUrl(url);
			Date dt = parseTime(time);
			Map<String, String> statMap = parseStatus(statusLatency);

			logParseMap.putAll(urlMap);
			logParseMap.putAll(statMap);
			logParseMap.put("logDateTime", dt);
			logParseMap.put("logDate", formatDate(dt));
			logParseMap.put("parameter", parameters);

			logParseMap.put("id", logLine.hashCode() + "");
		}
		return logParseMap;
	}

	private Object formatDate(Date dt) {
		Calendar cal = new GregorianCalendar();
		cal.setTime(dt);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		
		return sdf.format(dt)+"T00:00:00.000Z";
	}

	private SolrInputDocument createSolrInputDocument(Map<String, Object> urlMap) {
		SolrInputDocument inputDoc = new SolrInputDocument();

		for (Iterator<String> iterator = urlMap.keySet().iterator(); iterator
				.hasNext();) {
			String key = iterator.next();
			Object val = urlMap.get(key);
			inputDoc.addField(key, val);
		}
		return inputDoc;
	}

	private Map<String, String> parseStatus(String statusLatency) {
		Map<String, String> statMap = new HashMap<String, String>();
		String[] stats = statusLatency.split(" ");
		int i = 0;
		for (String stat : stats) {
			if (!"".equals(stat)) {
				switch (i) {
				case 0:
					statMap.put("status", stat);
					break;
				case 1:
					statMap.put("size", stat);
					break;
				case 2:
					statMap.put("latency", stat);
					break;
				default:
					break;
				}
				i++;
			}
		}
		return statMap;
	}

	private Date parseTime(String time) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
		Date dt = sdf.parse(time);
		return dt;
	}

	private Map<String, String> parseUrl(String url) {
		Map<String, String> urlMap = new HashMap<String, String>();
		@SuppressWarnings("deprecation")
		String decodedUrl = URLDecoder.decode(url);
		String[] urlArray = decodedUrl.trim().split("/");
		int i = 0;
		for (String urlComp : urlArray) {
			if (!"".equals(urlComp)) {
				switch (i) {
				case 2:
					urlMap.put("pos", urlComp);
					break;
				case 3:
					urlMap.put("locale", urlComp);
					break;
				case 4:
					urlMap.put("product", urlComp);
					break;
				default:
					break;
				}
				i++;
			}
		}
		urlMap.put("url", decodedUrl);
		return urlMap;
	}

	private String extractStatusLatency(String logLine) {
		String statLat = logLine.substring(logLine.lastIndexOf("&quot;") + 6,
				logLine.length() - 2);
		return statLat;
	}

	private String extractTime(String logLine) {
		String time = logLine.substring(logLine.indexOf("[") + 1,
				logLine.indexOf("]", logLine.indexOf("[")));

		return time;
	}

	public String extractUrl(String logLine) {
		String url = logLine.substring(logLine.indexOf("GET") + 3,
				logLine.indexOf("HTTP", logLine.indexOf("GET")));
		return url;

	}

	public boolean isPageContent(String logLine) {
		return logLine != null && logLine.contains("pagecontent");
	}

	public SolrServer createSolrServer() throws Exception {
		SolrServer solrServer = new CommonsHttpSolrServer(
				"http://localhost:8983/solr/core0/");
		return solrServer;
	}

	public void optimize() throws SolrServerException, IOException {
		solrServer.optimize();
	}

	public void addSolr(List<SolrInputDocument> docList)
			throws SolrServerException, IOException {

		UpdateResponse resp = solrServer.add(docList);
		System.out.println(resp);

		solrServer.commit();

	}

	protected String extractParameter(String logLine) {
		logLine = URLDecoder.decode(logLine);
		String parameter = "";
		if(logLine.lastIndexOf("|") != -1){
		 parameter = logLine.substring(logLine.lastIndexOf("|")+1,logLine.indexOf("HTTP"));
		}
		return parameter.trim();
	}

}
