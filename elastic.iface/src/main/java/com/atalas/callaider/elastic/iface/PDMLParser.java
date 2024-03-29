package com.atalas.callaider.elastic.iface;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.log4j.Logger;

import com.atalas.callaider.elastic.iface.ProtocolContainer.Description;
import com.atalas.callaider.elastic.iface.storage.FieldMapping;
import com.atalas.callaider.elastic.iface.storage.IdGenerator;
import com.atalas.callaider.elastic.iface.storage.FieldMapping.Type;

public class PDMLParser {

	private static Logger logger = Logger.getLogger(PDMLParser.class);
	private final IdGenerator idGenerator = new IdGenerator();

	public static interface PacketListener {
		public void processNextPacket(List<ProtocolContainer> packet);
	}

	private XMLInputFactory xmlInputFactory;
	private XMLStreamReader xmlStreamReader;
	private Map<String, ProtocolContainer.Description> protocolsStack;

	// ==========================================================================================================================

	public PDMLParser(InputStream is,
			Map<String, ProtocolContainer.Description> protocolsStack)
			throws XMLStreamException {
		xmlInputFactory = XMLInputFactory.newInstance();
		xmlStreamReader = xmlInputFactory.createXMLStreamReader(is);
		this.protocolsStack = protocolsStack;
	}

	// ==========================================================================================================================

	public void parse( PacketListener pl ) throws XMLStreamException{ 
				
		List<ProtocolContainer> protocolsL = new ArrayList<>();
		int currentProtoLevel = 0;
		String lastId = null, lastProto=null;
		Object timestamp = null;
		
		while(xmlStreamReader.hasNext()){
			if(XMLEvent.START_ELEMENT==xmlStreamReader.next() 
					&& xmlStreamReader.hasName() ) {
				
				String elemName = xmlStreamReader.getName().toString();
				
				if( elemName.equals("packet")){
					if(!protocolsL.isEmpty())
						pl.processNextPacket( protocolsL );
					protocolsL = new ArrayList<>();
					currentProtoLevel=0;
					lastId = null;
					lastProto = null;
					timestamp = null;
				
				} else if( elemName.equals("proto")){
						
					String protoName = xmlStreamReader.getAttributeValue(null, "name");
					if( protocolsStack.containsKey(protoName)){
						Description pd = protocolsStack.get(protoName);
						String id = idGenerator.nextId();
						String parentId = null;
						String parentProto = null;
						int level = pd.getLevel();
						if( level > currentProtoLevel){ //protocol should use last ID as parent
							parentId = lastId; 
							parentProto = lastProto;
							
						} else { //looking for the last lower level
							for( int idx = protocolsL.size()-1; idx >= 0; idx--) {
								ProtocolContainer oldProtoContainer = protocolsL.get(idx);
								if( oldProtoContainer.getDescription().getLevel() < level ){
									parentId = oldProtoContainer.getId();
									parentProto = oldProtoContainer.getDescription().getName();
								}
							}								
						}
						ProtocolContainer protocolCont = new ProtocolContainer( protoName, id, parentId, pd );
						parse(protocolCont, pd.getFields(), xmlStreamReader);
						if( timestamp == null ){
							timestamp = protocolCont.getField("x_timestamp");
						} else {
							try {
								protocolCont.setField("x_timestamp", timestamp);
							} catch (IOException e) {
								logger.error("Failed to set timestamp."+ e.getMessage(),e);									
							}
						}
								
						try {
							if( null!=parentProto) protocolCont.setField("parent", parentProto);
						} catch (IOException e) {
							e.printStackTrace();
						}
						protocolsL.add( protocolCont);
						lastId = id;
						lastProto = protoName;
						currentProtoLevel = level;
					}
				} //if elemName =
			} // if XMLEvent.START_ELEMENT
		} //while xmlStreamReader.hasNext()
		if(!protocolsL.isEmpty())
			pl.processNextPacket( protocolsL );
	}	// ==========================================================================================================================

	public List<List<ProtocolContainer>> parse() throws XMLStreamException {
		final List<List<ProtocolContainer>> packetList = new ArrayList<List<ProtocolContainer>>();
		parse(new PacketListener() {

			@Override
			public void processNextPacket(List<ProtocolContainer> packet) {
				packetList.add(packet);
			}
		});
		return packetList;
	}

	// ==========================================================================================================================

	private Map<String, Object> parse(ProtocolContainer pc,
			FieldMapping fieldsTree, XMLStreamReader cr)
			throws XMLStreamException {
		int ignoreCounter = 1;
		Map<String, Object> nextElem = new HashMap<>();
		while (cr.hasNext()) {

			switch (cr.next()) {
			case XMLEvent.END_ELEMENT:
				ignoreCounter--;
				if (0 == ignoreCounter)
					return nextElem;
				break;

			case XMLEvent.START_ELEMENT: // проверяем что элемент нам интересен
				if (ignoreCounter < 2) {
					String name = cr.getAttributeValue(null, "name");
					if ("".equals(name))
						name = "x";
					FieldMapping llElemnt = fieldsTree.lowerLevelMap == null ? null
							: fieldsTree.lowerLevelMap.get(name);
					if (null != llElemnt) {// интересный элемент
						if (Type.CONTAINER == llElemnt.type) {
							if( null!=llElemnt.shortName){
								try {
									setTheField(pc, llElemnt.shortName, name, llElemnt);
								} catch (NumberFormatException | IOException e) {
									logger.error(
											"Failed to setField: " + name
													+ " as a container presence flag: "
													+ e.getMessage(), e);
									e.printStackTrace();
								}
								
							}
							parse(pc, llElemnt, cr);
							ignoreCounter--;
						} else {
							String val = xmlStreamReader.getAttributeValue(
									null, "show");
							try {
								setTheField(pc, name, val, llElemnt);
							} catch (NumberFormatException | IOException e) {
								logger.error(
										"Failed to setField: " + name
												+ " value:" + val + " : "
												+ e.getMessage(), e);
								e.printStackTrace();
							}
						}
					} else { // неинтересный элемент
					}
				}
				ignoreCounter++;
				break;
			default:
				break;

			}
		}
		return nextElem;
	}

	// ==========================================================================================================================

	private void setTheField(ProtocolContainer pc, String fielldName,
			String fieldVal, FieldMapping fielsMap)
			throws NumberFormatException, IOException {
		if (fielsMap.shortName != null)
			fielldName = fielsMap.shortName;

		if (null != fieldVal) {
			if (Type.INT == fielsMap.type) {
				pc.setField(fielldName, Long.parseLong(fieldVal));
			
			} else if (Type.FLOAT == fielsMap.type) {
				pc.setField(fielldName, Double.parseDouble(fieldVal));
			} else if (Type.CONTAINER == fielsMap.type) {
				pc.setField(fielldName, fieldVal);			
			} else if (Type.DATE == fielsMap.type) {
				Calendar cldr = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
				cldr.setTimeInMillis(Long.parseLong(fieldVal.substring(0, 10))
						* 1000 + Integer.parseInt(fieldVal.substring(11, 14)));
				pc.setField(fielldName, cldr.getTime());
			} else if (Type.FLAG == fielsMap.type) {
				pc.setField(fielldName, Boolean.TRUE);
			}else {
				pc.setField(fielldName, fieldVal);
			}
		}
	}
}
