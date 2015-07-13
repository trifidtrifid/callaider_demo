package com.atalas.callaider.elastic.iface;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.atalas.callaider.elastic.iface.McidFlow.GsmTransaction;

public class McidPacketProcessor extends PacketProcessor {

	public McidPacketProcessor(Client client, String indexName) {
		super(client, indexName);
	}

	@Override
	public void processNextPacket(List<ProtocolContainer> packet) {
		super.processNextPacket(packet);

		for (ProtocolContainer proto : packet) {
			if (proto.getDescription().getName() == "gsm_map") {
				String elementType = proto.getTheField("map_component_type");
				switch (elementType) {

				case "gsm_old.invoke_element":
					processInvoke(proto);
					break;
				case "gsm_old.returnResultLast_element":
					processReturnResultLast(proto);
					break;
				case "gsm_old.returnError_element":
					processReturnError(proto);
					break;
				default:
					logger.debug(" unsupported map element type: " + elementType);
					break;
				}
			}
		}
	}

	void processReturnError(ProtocolContainer proto) {
	}

	void processReturnResultLast(ProtocolContainer proto) {
		int opCode = proto.getTheField("opCode");
		switch (opCode) {
		case 22:
		case 45:
			processSriResp(proto);
			break;
		case 70:
			processPsiResp(proto);
		default:
			logger.debug("unsupported map opcode in responce: " + opCode);
			break;
		}
	}
	
	void processInvoke(ProtocolContainer proto) {
		int opCode = proto.getTheField("opCode");
		switch (opCode) {
		case 22:
		case 45:
			processSriReq(proto);
			break;
		case 70:
			processPsiReq(proto);
		default:
			logger.debug("unsupported map opcode in request: " + opCode);
			break;
		}
	}

	void processPsiResp(ProtocolContainer proto){
		//todo get McidFlow by "PSI.m_request.tid" 
		String tid = proto.getTheField("tid");
		
		McidFlow mcidFlow = new McidFlow();
		if(mcidFlow == null){
			logger.warn("can't find SRI req by tid: " + tid);
			return;
		}
		
		McidFlow.GsmTransaction gsmTransaction = mcidFlow.m_transactions.get("PSI");
		if(gsmTransaction == null){
			logger.warn("can't find SRI transaction by tid: " + tid);
			return;
		}
		String laiOrCid = proto.getTheField("CID_LAI_SAI");
		String result = "success";
		if(laiOrCid.isEmpty())
			result = "fault";
		
		gsmTransaction.m_response.put("result", result);
		
		//todo update
	}


	void processPsiReq(ProtocolContainer proto) {
		//todo get McidFlow by "SRI.m_response.imsi" 
		String imsi = proto.getTheField("imsi");
		
		McidFlow mcidFlow = new McidFlow();
		if(mcidFlow == null){
			logger.warn("can't find PSI req by imsi: " + imsi);
			return;
		}
		
		McidFlow.GsmTransaction gsmTransaction = new McidFlow.GsmTransaction();
		String tid = proto.getTheField("tid");
		if(tid.isEmpty()){
			logger.warn("SRI request have no tid");
			return;
		}
		
		gsmTransaction.m_request.put("tid", tid);
		mcidFlow.m_transactions.put("PSI", gsmTransaction);
		
		//todo update
	}

	void processSriResp(ProtocolContainer proto){
		//todo get McidFlow by "SRI.m_request.tid" 
		String tid = proto.getTheField("tid");
		
		McidFlow mcidFlow = new McidFlow();
		if(mcidFlow == null){
			logger.warn("can't find SRI req by tid: " + tid);
			return;
		}
		
		McidFlow.GsmTransaction gsmTransaction = mcidFlow.m_transactions.get("SRI");
		if(gsmTransaction == null){
			logger.warn("can't find SRI transaction by tid: " + tid);
			return;
		}
		String imsi = proto.getTheField("imsi");
		gsmTransaction.m_response.put("imsi", imsi);
		
		//todo update
	}
	
	void processSriReq(ProtocolContainer proto) {
		McidFlow mcidFlow = new McidFlow();
		McidFlow.GsmTransaction gsmTransaction = new McidFlow.GsmTransaction();
		String tid = proto.getTheField("tid");
		if(tid.isEmpty()){
			logger.warn("SRI request have no tid");
			return;
		}
		
		gsmTransaction.m_request.put("tid", tid);
		mcidFlow.m_transactions.put("SRI", gsmTransaction);
		
		//todo save
	}

	static Logger logger = Logger.getLogger(McidPacketProcessor.class.getName());

}
