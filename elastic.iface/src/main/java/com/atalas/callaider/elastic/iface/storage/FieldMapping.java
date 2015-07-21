package com.atalas.callaider.elastic.iface.storage;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.atalas.callaider.elastic.iface.ProtocolContainer;
import com.atalas.callaider.elastic.iface.ProtocolContainer.Description;

public class FieldMapping {
	
	private static Logger logger = Logger.getLogger(FieldMapping.class.getName());
	
	private FieldMapping(Type type, Map<String, FieldMapping> lowerLevelMap, String fieldName) {
		this.type = type;
		this.lowerLevelMap = lowerLevelMap;
		this.shortName = fieldName;	
	}
	
	public static enum Type { INT, STRING, DATE, FLOAT, CONTAINER };
	public String shortName;
	public Type type;		
	public Map<String,FieldMapping> lowerLevelMap;
	
	public static Map<String,ProtocolContainer.Description> loadProtocoMapping( String fileName ) throws IOException {
		
		Map<String,Description> pdm = new HashMap<String, ProtocolContainer.Description>();
		
		BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream(fileName)));		
		String line;
		Description currentProtocol = null;
		while( null!=(line=br.readLine())){
			if(line.startsWith("proto")){				
				currentProtocol = parseProtocolDescription(pdm, line);
			} else {
				if(null!=currentProtocol){
					parseString( line, currentProtocol.getFields() );
				} else if( !line.trim().isEmpty()){
					logger.warn("Skipped line: '"+line+"' in configuration file. No protocol description 'proto <name> <level>' found before.");
				}
			}			
		}
		return pdm;
	}
	
	private static Description parseProtocolDescription(Map<String, Description> pdm, String line) {
		String[] parts = line.split("[ ]");
		String name = parts[1];
		Description oldDescr = pdm.get(name);
		if(null!=oldDescr)
			return oldDescr;
		
		Description newDesc = new Description(name, Integer.parseInt(parts[2]), 
				new FieldMapping(Type.CONTAINER, new HashMap<String, FieldMapping>(), name));
		pdm.put(name, newDesc);
		return newDesc;
	}

	private static void parseString( String descLine, FieldMapping currentMapping ){
		if(!descLine.trim().startsWith("#")){
			String[] parts = descLine.split("[ ]");
			if( parts.length > 1) 
				postThePart( currentMapping.lowerLevelMap, parts, 0);		
		}
	}

	private static void postThePart( Map<String,FieldMapping> fieldMap, String[] parts, int offset) {
		String name,typeVal,shortName;
		name = typeVal = shortName = null;
		
		if( parts.length - offset >= 2){
			typeVal = parts[offset+1];
			name = parts[offset];
		}
			
		if( parts.length - offset == 2 ){ //field name and field type
			typeVal = parts[offset+1];
			if(-1!=typeVal.indexOf(":")){
				shortName = typeVal.substring(typeVal.indexOf(":")+1);
				typeVal = typeVal.substring(0,typeVal.indexOf(":"));				
			}
			fieldMap.put( "\"\"".equals(name) ? "x" : name, 
					new FieldMapping(Type.valueOf(typeVal), null, shortName));
		} if( parts.length - offset > 2 ){

			if(-1!=name.indexOf(":")){
				shortName = name.substring(name.indexOf(":")+1);
				name = name.substring(0,name.indexOf(":"));				
			}
			FieldMapping childMap = fieldMap.get(name);
			if( null == childMap ){
				childMap = new FieldMapping(Type.CONTAINER, new HashMap<String, FieldMapping>(), shortName);
				fieldMap.put(name,childMap);				
			}
			postThePart(childMap.lowerLevelMap, parts, offset+1);
		}
	}
}
