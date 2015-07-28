package com.atalas.callaider.elastic.iface;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.atalas.callaider.elastic.iface.storage.StorageInterface;
import com.atalas.callaider.flow.mcid.McidFlow;

public class StorageInterfaceTest extends TestCase {

	private final String index = "test";
	private Client client;
	
	private Client getClient() {
        final ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        TransportClient client = new TransportClient(settings)
        	.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		return client;
    }

	private void dropTheIndex(Client client) {
		final IndicesExistsResponse res = client.admin().indices().prepareExists(index).execute().actionGet();
		if (res.isExists()) {
		    final DeleteIndexRequestBuilder delIdx = client.admin().indices().prepareDelete(index);
		    delIdx.execute().actionGet();
		}
	}
	
	public void testSearchObjects() {
				
		McidFlow mcf = createTEstObject();
				
        dropTheIndex(client);
        
		StorageInterface si = new StorageInterface(client, index);
		
		String mappStr = si.createObjectMapping( McidFlow.class );		
		
		String id = si.saveObject(mcf);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}		
		
		//search test
		Map<String,Object> searchMap;
		searchMap = new HashMap<>();
		searchMap.put("request.a", 1);
		List<McidFlow> foundObjs = si.searchObjects(McidFlow.class, searchMap);
		assertEquals( foundObjs.size(), 0 );
		
		searchMap = new HashMap<>();
		searchMap.put("sri.request.a", 1);
		foundObjs = si.searchObjects(McidFlow.class, searchMap);		
		assertEquals( foundObjs.size(), 1);
		assertEquals( foundObjs.get(0).id, id );
		
		searchMap = new HashMap<>();
		searchMap.put("psi.request.a", 1);
		foundObjs = si.searchObjects(McidFlow.class, searchMap);
		assertEquals( foundObjs.size(), 0);
		
		searchMap = new HashMap<>();
		searchMap.put("sri.response.c", true);
		foundObjs = si.searchObjects(McidFlow.class, searchMap);
		assertEquals( foundObjs.size(), 1);			
	}

	private McidFlow createTEstObject() {
		McidFlow mcf = new McidFlow();
		//mcf.m_transactions = new HashMap<>();
		McidFlow.GsmTransaction gt1 = new McidFlow.GsmTransaction();

		gt1.request = new HashMap<>();
		gt1.response = new HashMap<>();
		gt1.request.put("a", 1);
		gt1.request.put("b", 1.0D);
		gt1.request.put("c", true);
		
		gt1.response.put("a", 1);
		gt1.response.put("b", 1.0D);
		gt1.response.put("c", true);		
		
		McidFlow.GsmTransaction gt2 = new McidFlow.GsmTransaction();
		gt2.request = new HashMap<>();
		gt2.response = new HashMap<>();
		gt2.request.put("a", 2);
		gt2.request.put("b", 2.0D);
		gt2.request.put("c", false);		
		
		mcf.sri = gt1;
		mcf.psi = gt2;
		
		return mcf;
	}

	public void testSaveObject() {
		//fail("Not yet implemented");
		
	}

	public void testUpdateObject() {
		testSaveObject();
	}

	protected void setUp() throws Exception {
		client = getClient();
		super.setUp();
	}

	protected void tearDown() throws Exception {
		if( null!=client) client.close();
		super.tearDown();
	}
}
