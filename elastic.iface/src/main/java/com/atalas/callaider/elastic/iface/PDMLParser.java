package com.atalas.callaider.elastic.iface;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import com.atalas.callaider.elastic.iface.PDMLParser.FieldMapping.Type;

public class PDMLParser {
	
	public static interface PacketListener {
		public void processNextPacket(Map<String, Object> packet);
	}
	public static class FieldMapping {
		public FieldMapping(Type type, Map<String, FieldMapping> lowerLevelMap) {
			this.type = type;
			this.lowerLevelMap = lowerLevelMap;
		}
		public static enum Type { INT, STRING, DATE, FLOAT, CONTAINER };
		public Type type;		
		public Map<String,FieldMapping> lowerLevelMap;
	}
	
	private XMLInputFactory f;
	private XMLStreamReader r;
	private FieldMapping fieldsTree;

	public PDMLParser( InputStream is, FieldMapping fieldsTree) throws XMLStreamException{
		f = XMLInputFactory.newInstance();
		r = f.createXMLStreamReader( is );
		this.fieldsTree = fieldsTree;
	}
	
	public void parse( PacketListener pl ) throws XMLStreamException{ 
		while(r.hasNext()){
			if(XMLEvent.START_ELEMENT==r.next() 
					&& r.hasName() && r.getName().toString().equals("packet")){
				pl.processNextPacket( parse(fieldsTree,r));				
			} 
		}
	} 
	
	public List<Map<String,Object>> parse() throws XMLStreamException{ /*будет запускать в потоке */
		final List<Map<String,Object>> packetList = new ArrayList<Map<String,Object>>();
		parse( new PacketListener() {
			
			@Override
			public void processNextPacket(Map<String, Object> packet) {
				packetList.add(packet);				
			}
		});
		return packetList;
	} 
	
	//==========================================================================================================================
		
	/*запускается в рекурсии*/
	private Map<String,Object> parse(FieldMapping fieldsTree, XMLStreamReader cr) throws XMLStreamException{ 
		int ignoreCounter = 1;
		Map<String,Object> nextElem = new HashMap<>();
		while(cr.hasNext()) {
			
		    switch (cr.next()){
		    case XMLEvent.END_ELEMENT:
		    	ignoreCounter--;
		    	if(0==ignoreCounter)
		    		return nextElem;
		    	break;
		    	
		    case XMLEvent.START_ELEMENT: // проверяем что элемент нам интересен
		    	if(ignoreCounter<2){
			    	String name = cr.getAttributeValue(null, "name");	
			    	if( "".equals(name))
			    		name = "x";
			    	FieldMapping llElemnt = fieldsTree.lowerLevelMap == null ? null : fieldsTree.lowerLevelMap.get(name);
					if(null!=llElemnt){//интересный элемент
						if( Type.CONTAINER == llElemnt.type ){
							Map<String, Object> llInfo = parse( llElemnt, cr);
							if( !llInfo.isEmpty()){
								appendChildElement(nextElem, name, llInfo);									
							}
							ignoreCounter--;
						} else {
							String val = r.getAttributeValue(null, "show");
							fieldContainer(nextElem, name, val, llElemnt);
						}
					} else { //неинтересный элемент					
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

	//==========================================================================================================================
	
	private void fieldContainer(Map<String, Object> fieldContainer,	String fielldName, String fieldVal, FieldMapping fielsMap) {
		
		if( null!=fieldVal) {
			if( Type.INT == fielsMap.type ){
				fieldContainer.put(fielldName, Long.parseLong(fieldVal));
			} else if( Type.FLOAT == fielsMap.type ){
				fieldContainer.put(fielldName, Double.parseDouble(fieldVal));
			} else if( Type.DATE == fielsMap.type ){
				fieldContainer.put(fielldName, Date.parse(fieldVal));
			} else {
				fieldContainer.put(fielldName, fieldVal);
			}
		}
	}

	//==========================================================================================================================
	
	private void appendChildElement(Map<String, Object> parentElement,
			String name, Map<String, Object> childElement) {
		Object oldValue = parentElement.get(name);
		if( null==oldValue )
			parentElement.put(name,childElement);
		else {// заменяем списком
			if( oldValue instanceof List){
				((List)oldValue).add(childElement);
			} else {
				List elemList = new ArrayList<Object>();
				elemList.add(oldValue);
				elemList.add(childElement);
				parentElement.put(name,elemList);
			}
		}
	}
}
