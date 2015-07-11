package com.atalas.callaider.elastic.iface;

import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;

import com.atalas.callaider.elastic.iface.PDMLParser.PacketListener;


public class McidPacketProcessor extends PacketProcessor {

	public McidPacketProcessor(Client client) {
		super(client);
	}

	@Override
	public void processNextPacket(Map<String, Object> packet) {

/*		Object value = packet.get("sccp");
	if( value instanceof List ) {
			for( Object obj: (List)value){
				processField(contentBuilder, obj, name); 
			}
		} if( value instanceof Map ) {
			for( Map.Entry<String,Object> obje: ((Map<String,Object>)value).entrySet() ){
				contentBuilder.startObject(name);
				processField(contentBuilder, obje.getValue(), obje.getKey()); 
				contentBuilder.endObject();
			}
		} else { //it's a simple field				
			contentBuilder.field(name, value);
		}
*/
		super.processNextPacket(packet);
	}





}
