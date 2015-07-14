package com.atalas.callaider.elastic.iface;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import junit.framework.TestCase;

public class StorageInterfaceTest extends TestCase {

	private static final String index = "test";
	private static Client getClient() {
        final ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        TransportClient transportClient = new TransportClient(settings)
        	.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        return transportClient;
    }
	
	public void testSearchObjects() {
		Client client = getClient();
		StorageInterface si = new StorageInterface(client, null, index);
		
		
	}

	public void testSaveObject() {
		//fail("Not yet implemented");
		
	}

	public void testUpdateObject() {
		testSaveObject();
	}

}
