package com.atalas.callaider.flow;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import com.atalas.callaider.elastic.iface.storage.FieldMapping;
import com.google.gson.Gson;

public class ProtocolMappedFieldsHelper {
	
	private static Map< String, Map<String,FieldMapping.Type>> protoFieldsMap = new HashMap<>();
	private static Map< String, Map<String,Type>> protoFieldsRealMap = new HashMap<>();
	
	public static void setFieldMapping( String protoName, FieldMapping fm){
		Map<String,FieldMapping.Type> typeMap = new HashMap<>();
		Map<String,Type> realTypesMap = new HashMap<>();
		addAllFields( typeMap, realTypesMap, fm);
		protoFieldsMap.put( protoName, typeMap);
		protoFieldsRealMap.put(protoName, realTypesMap);		
	}
	
	private static void addAllFields( Map<String,FieldMapping.Type> typeMap, Map<String,Type> realTypes, FieldMapping fm){
		for( Entry<String, FieldMapping> ne : fm.lowerLevelMap.entrySet()){
			FieldMapping value = ne.getValue();
			if( null==value) continue;
			String name = value.shortName == null ? ne.getKey() : value.shortName;			
			
			if( FieldMapping.Type.CONTAINER == value.type ){
				if( null!=value.shortName ){
					typeMap.put(value.shortName, FieldMapping.Type.STRING);
					realTypes.put(value.shortName, String.class);
				}
				addAllFields( typeMap, realTypes, value);
				
			} else {
				typeMap.put(name, value.type);
				
				switch (value.type){
				case DATE:
					realTypes.put(name, Date.class);
				case INT:
					realTypes.put(name, Long.class);
				case FLOAT:
					realTypes.put(name, Double.class);
					default:
						realTypes.put(name, String.class);
						break;
				} 
			}
		}
	}
	
	public static Object getFieldValue( Map<String, Object> data, String fieldName, String protoName ){
		Object fieldObject = data.get(fieldName);
		Map<String, FieldMapping.Type> ptm = protoFieldsMap.get(protoName);
		if( null!=ptm) {
			FieldMapping.Type type = ptm.get(fieldName);
			switch (type){
			case DATE:
				return fieldObject instanceof Date ? fieldObject : new Date(Date.parse( "" + fieldObject));
			case INT:
				return Long.parseLong(""+fieldObject);
				
			case FLOAT:
				return Double.parseDouble(""+fieldObject);
				default:
					return ""+fieldObject;					
			} 
		} 
		return data.get(fieldName);			
	}
}
