package com.atalas.callaider.flow.mcid;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;

import com.atalas.callaider.elastic.iface.storage.StorageInterface;

import junit.framework.TestCase;

public class MLBSweb_app_logParserTest extends TestCase {

	MLBSweb_app_logParser walp;
	StorageInterface si;
	String indexName = "test";
	
	protected void setUp() throws Exception {
		
		walp = new MLBSweb_app_logParser(indexName);
		si = new StorageInterface(indexName);
		
	}

	protected void tearDown() throws Exception {
		final IndicesExistsResponse res = si.getClient().admin().indices().prepareExists(indexName).execute().actionGet();
		if (res.isExists()) {
		    final DeleteIndexRequestBuilder delIdx = si.getClient().admin().indices().prepareDelete(indexName);
		    delIdx.execute().actionGet();
		}
		super.tearDown();
	}

	public void testProcessTheEvent() {
		McidFlow mcf = new McidFlow();
		String msisdn = "123456";
		Date theDate = new Date();
		String uid = "23456";		
		
		mcf.x_msisdn=msisdn;
		mcf.x_timestamp = theDate;
		si.saveObject(mcf);		
		walp.processTheEvent(msisdn, uid, theDate);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {			
			e.printStackTrace();
		}
		List<McidFlow> mcfF = si.searchObjects(McidFlow.class, new HashMap<>());
		assertEquals(1, mcfF.size());
		McidFlow mcidFlow = mcfF.get(0);
		assertEquals(mcidFlow.x_msisdn, msisdn);
		assertEquals(mcidFlow.x_client, uid);				
	}

}
