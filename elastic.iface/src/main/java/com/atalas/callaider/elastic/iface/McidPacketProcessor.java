package com.atalas.callaider.elastic.iface;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.List;
import java.util.Map.Entry;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;


public class McidPacketProcessor extends PacketProcessor {

	public McidPacketProcessor(Client client, String indexName) {
		super(client, indexName);		
	}

	@Override
	public void processNextPacket(List<ProtocolContainer> packet) {
		super.processNextPacket(packet);

		for(ProtocolContainer proto : packet){
			if(proto.getDescription().getName() == "gsm_map"){
/*				switch(proto.getField("opCode")){
				
				case 22 :
					break;
					
				
				}
*/				
			}
		}
	}
	
	
	void processSriReq(ProtocolContainer proto){
      	
      /*	final XContentBuilder contentBuilder = jsonBuilder().startObject().prettyPrint();
      	if( null!=protocol.getParentId()){
      		contentBuilder.field("parentId", protocol.getParentId());
      	}
      	for( Entry<String, Object> field :protocol.getFieldsEntrySet()){
        	contentBuilder.field(field.getKey(), field.getValue());
      	}
      	contentBuilder.endObject();
        indexRequestBuilder.setSource(contentBuilder);
        
        Object tsfld = protocol.getField("@timestamp");
        if(null!=tsfld) 
        	indexRequestBuilder.setTimestamp( sdf.format(tsfld));
        
        AppXML.logger.debug(docType+"["+protocol.getId()+"] described as:"+contentBuilder.string());
        brb.add(indexRequestBuilder);
      */}
      
	
}
