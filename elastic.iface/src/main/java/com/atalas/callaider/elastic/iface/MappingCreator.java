package com.atalas.callaider.elastic.iface;

import java.util.Map.Entry;

import com.atalas.callaider.elastic.iface.FieldMapping.Type;
import com.atalas.callaider.elastic.iface.ProtocolContainer.Description;


public class MappingCreator {

	public static String createMapping( String typeName, Description description){
		String mappingString = "{\"" + typeName + "\" : { \"properties\" : {";
		
		mappingString = processTheFIeldMap(description.getFields(), mappingString);
		
		if( mappingString.endsWith(","))
			mappingString = mappingString.substring(0,mappingString.length()-1);
		mappingString += "}}}";
		return mappingString;
	}

	private static String processTheFIeldMap(FieldMapping mapping,
			String mappingString) {
		for( Entry<String,FieldMapping> fme : mapping.lowerLevelMap.entrySet() ){
			FieldMapping value = fme.getValue();
			if( value.type == Type.CONTAINER && null!=value.lowerLevelMap && !value.lowerLevelMap.isEmpty()){
				mappingString = processTheFIeldMap(value, mappingString);
			} else {
				String name = fme.getValue().shortName;
				if (null == name) 
					name = fme.getKey();
				
				mappingString += "\""+name+"\":{ \"index\":\"not_analyzed\", \"type\": \"";
				switch( value.type){
				case INT:	
					mappingString += "integer\"";
					break;
				case DATE:
					mappingString += "date\"";
					break;
				case FLOAT:					
					mappingString += "float\"";
					break;
				default: 				
					mappingString += "string\"";
					break;
				}
				mappingString += "},";
			}
						
		}
		return mappingString;
	}

}
