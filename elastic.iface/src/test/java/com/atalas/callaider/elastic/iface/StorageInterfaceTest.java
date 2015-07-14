package com.atalas.callaider.elastic.iface;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.atalas.callaider.elastic.iface.storage.StorageInterface;
import com.atalas.callaider.flow.mcid.McidFlow;
import com.atalas.callaider.flow.mcid.McidFlow.GsmTransaction;

import junit.framework.TestCase;

public class StorageInterfaceTest extends TestCase {

	private final String index = "test";
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
		
		Client client = getClient();
        dropTheIndex(client);
        
		StorageInterface si = new StorageInterface(client, null, index);
		String id = si.saveObject(mcf);
		
		//search test
		Map<String,Object> searchMap = new HashMap<>();
		searchMap.put("rsp1_dblVal", 1);
		List<McidFlow> foundObjs = si.searchObjects(McidFlow.class, searchMap);
		assertEquals( foundObjs.size(), 1);
		
		searchMap = new HashMap<>();
		searchMap.put("secTrans.req1_dblVal", 2);
		foundObjs = si.searchObjects(McidFlow.class, searchMap);
		assertEquals( foundObjs.size(), 1);
		
		searchMap = new HashMap<>();
		searchMap.put("secTrans.req1_dblVal", 1);
		foundObjs = si.searchObjects(McidFlow.class, searchMap);
		assertEquals( foundObjs.size(), 0);
		
		
	}

	private McidFlow createTEstObject() {
		McidFlow mcf = new McidFlow();
		/*mcf.m_transactions = new HashMap<>();
		GsmTransaction gt1 = new GsmTransaction();
		gt1.m_request = new HashMap<>();
		gt1.m_response = new HashMap<>();
		gt1.m_request.put("req1_intVal", 1);
		gt1.m_request.put("req1_dblVal", 1.0D);
		gt1.m_request.put("req1_boolVal", true);
		gt1.m_request.put("req1_stringVal", "1");
		
		gt1.m_response.put("rsp1_intVal", 1);
		gt1.m_response.put("rsp1_dblVal", 1.0D);
		gt1.m_response.put("rsp1_boolVal", true);
		gt1.m_response.put("rsp1_stringVal", "1");
		
		GsmTransaction gt2 = new GsmTransaction();
		gt2.m_request = new HashMap<>();
		gt2.m_response = new HashMap<>();
		gt2.m_request.put("req2_intVal", 2);
		gt2.m_request.put("req1_dblVal", 2.0D);
		gt2.m_request.put("req1_boolVal", false);
		gt2.m_request.put("req1_stringVal", "2");
		
		mcf.m_transactions.put("firstTrans", gt1);
		mcf.m_transactions.put("secTrans", gt2);*/
		return mcf;
	}

	public void testSaveObject() {
		//fail("Not yet implemented");
		
	}

	public void testUpdateObject() {
		testSaveObject();
	}

}
