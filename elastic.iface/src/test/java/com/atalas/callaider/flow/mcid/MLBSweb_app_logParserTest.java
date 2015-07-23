package com.atalas.callaider.flow.mcid;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;

import com.atalas.callaider.elastic.iface.storage.StorageInterface;

public class MLBSweb_app_logParserTest extends TestCase {

	MLBSweb_app_logParser walp;
	StorageInterface si;
	String indexName = "test";
	
	protected void setUp() throws Exception {
		
		walp = new MLBSweb_app_logParser(indexName);
		si = new StorageInterface(indexName);
		BasicConfigurator.configure();
		
		final IndicesExistsResponse res = si.getClient().admin().indices().prepareExists(indexName).execute().actionGet();
		if (res.isExists()) {
		    final DeleteIndexRequestBuilder delIdx = si.getClient().admin().indices().prepareDelete(indexName);
		    delIdx.execute().actionGet();
		}
	}

	public void testProcessTheEvent() {
		McidFlow mcf = new McidFlow();
		String msisdn = "123456";
		Date theDate = new Date();
		String uid = "23456";		
		
		mcf.x_msisdn=msisdn;
		mcf.x_timestamp = theDate;
		si.saveObject(mcf);
		
		McidFlow mcf2 = new McidFlow();
		mcf2.x_msisdn=msisdn;
		mcf2.x_timestamp = new Date( theDate.getTime() - 190000L);
		si.saveObject(mcf2);
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {			
			e.printStackTrace();
		}
		walp.processTheEvent(msisdn, uid, theDate, "0","0","0");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {			
			e.printStackTrace();
		}
		List<McidFlow> mcfF = si.searchObjects(McidFlow.class, new HashMap<>(),"x_timestamp", true);
		assertEquals(2, mcfF.size());
		McidFlow mcidFlow = mcfF.get(0);
		assertEquals(mcidFlow.x_msisdn, msisdn);
		assertEquals(mcidFlow.x_client, uid);				
	}

	public static class ParseREsults {
		public ParseREsults(String msisdn, String uid, Date date) {			
			this.msisdn = msisdn;
			this.uid = uid;
			this.date = date;
		}
		String msisdn;
		String uid;
		Date date;
	}
	
	public void testParseLog() {
		final List<ParseREsults> results = new ArrayList<>();
		walp = new MLBSweb_app_logParser(indexName){

			@Override
			public void processTheEvent(String msisdn, String uid, Date reqDate, String lon, String lat, String err) {
				results.add( new ParseREsults(msisdn,uid,reqDate));
			}
		};
		
		try {
			walp.parseLogs( new FileInputStream("src/test/resources/web_app.test.log"));
			assertEquals(10, results.size());
			ParseREsults theFIrstLine = results.get(0);
			assertEquals("79275696404",theFIrstLine.msisdn);
			assertEquals("controlcad",theFIrstLine.uid);
			assertEquals( new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"
					).parse("2015-07-22 04:02:09"),theFIrstLine.date);
			
		} catch (Exception e) {
			
			e.printStackTrace();
		}		 						
	}

}
