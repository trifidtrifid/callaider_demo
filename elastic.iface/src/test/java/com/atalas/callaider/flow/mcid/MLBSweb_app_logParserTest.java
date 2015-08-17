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
		Long msisdn = 79275696404L;
		Date theDate = new Date();
		String uid = "controlcad";		
		
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
		String line= "22/07/2015 04:02:09,REQ_ID='20150722040207.8894s10.236.26.210p49084',UID='controlcad',PWD='AbrU43k',MCC='',MNC='',CID='',LAC='',MODE='4',MSISDN='79275696404',IP=10.236.26.210,RESULT=OK,longitude=47.976700,latitude=46.422264,error=284,age=0,cid=38953,lac=3005,,comment=\\\"<LocationDatacomment=\\\"\\\"ageOfLocation=\\\"0\\\"locationInformation=\\\"-1\\\"/>";
		
		LBSLogRecord lr = walp.createLogRecord( line.split("[,]"));		
		walp.processTheEvent( lr);//msisdn, uid, theDate, "1","1","2","3","OK");
		
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
		public ParseREsults(Long msisdn, String uid, Date date) {			
			this.msisdn = msisdn;
			this.uid = uid;
			this.date = date;
		}
		Long msisdn;
		String uid;
		Date date;
	}
	
	public void testParseLog() {
		final List<ParseREsults> results = new ArrayList<>();
		walp = new MLBSweb_app_logParser(indexName){

			
			@Override
			public void processTheEvent(LBSLogRecord lr) {
				results.add( new ParseREsults(lr.msisdn,lr.uid,lr.x_timestamp));
			}			
		};
		
		try {
			walp.parseLogs( new FileInputStream("src/test/resources/web_app.test.log"));
			assertEquals(10, results.size());
			ParseREsults theFIrstLine = results.get(0);
			assertEquals(new Long(79275696404L),theFIrstLine.msisdn);
			assertEquals("controlcad",theFIrstLine.uid);
			assertEquals( new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"
					).parse("2015-07-22 04:02:09"),theFIrstLine.date);
			
		} catch (Exception e) {
			
			e.printStackTrace();
		}		 						
	}

}
