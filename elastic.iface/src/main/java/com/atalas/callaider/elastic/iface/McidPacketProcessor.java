package com.atalas.callaider.elastic.iface;

import org.elasticsearch.client.Client;


public class McidPacketProcessor extends PacketProcessor {

	public McidPacketProcessor(Client client, String indexName) {
		super(client, indexName);		
	}

	
}
