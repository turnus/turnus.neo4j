/* 
 * TURNUS - www.turnus.co
 * 
 * Copyright (C) 2010-2016 EPFL SCI STI MM
 *
 * This file is part of TURNUS.
 *
 * TURNUS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * TURNUS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with TURNUS.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Additional permission under GNU GPL version 3 section 7
 * 
 * If you modify this Program, or any covered work, by linking or combining it
 * with Eclipse (or a modified version of Eclipse or an Eclipse plugin or 
 * an Eclipse library), containing parts covered by the terms of the 
 * Eclipse Public License (EPL), the licensors of this Program grant you 
 * additional permission to convey the resulting work.  Corresponding Source 
 * for a non-source form of such a combination shall include the source code 
 * for the parts of Eclipse libraries used as well as that of the  covered work.
 * 
 */
package turnus.neo4j.trace;

import java.util.Map;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import turnus.model.trace.Dependency.Kind;

/**
 * 
 * @author Simone Casale Brunet
 *
 */
public class NeoConstants {
	
	public static enum TopoType implements RelationshipType {
		TOPOLOGICAL
	}

	public static enum RelType implements RelationshipType {
		FSM, GUARD, PORT, TOKENS, VARIABLE, MERGED, SCHEDULER, UNKONW;

		private static final BiMap<Kind, RelType> typesMap = HashBiMap.create();

		static {
			typesMap.put(Kind.FSM, FSM);
			typesMap.put(Kind.GUARD, GUARD);
			typesMap.put(Kind.PORT, PORT);
			typesMap.put(Kind.TOKENS, TOKENS);
			typesMap.put(Kind.VARIABLE, VARIABLE);
			typesMap.put(Kind.MERGED, MERGED);
			typesMap.put(Kind.SCHEDULER, SCHEDULER);
			typesMap.put(Kind.UNKNOWN, RelType.UNKONW);
		}

		public static RelType getRelType(Kind kind) {
			return typesMap.get(kind);
		}
		
		public static Kind getKind(RelationshipType type) {
			return typesMap.inverse().get(type);
		}
	}

	public static Map<String, String> NEO4J_DEFAULT_CONF = MapUtil.stringMap( //
			"cache_type", "weak", //
			"neostore.nodestore.db.mapped_memory", "500M", //
			"neostore.relationshipstore.db.mapped_memory", "2000M", //
			"neostore.propertystore.db.mapped_memory", "1000M", //
			"neostore.propertystore.db.strings.mapped_memory", "0M", //
			"neostore.propertystore.db.arrays.mapped_memory", "0M");
	
	public static int MAX_TRANSACTIONS = 500000;
	public static int CACHE_STEPS_MAP = 100000;
	public static int CACHE_DEPENDENCIES_MAP = 100000;

	public static final String TURNUS_PROPERTY = "_turnus_";

	public static final String LB_STEP = TURNUS_PROPERTY.concat("step");

	public static final String SP_ACTOR = TURNUS_PROPERTY.concat("actor");
	public static final String SP_ID = TURNUS_PROPERTY.concat("id");
	public static final String SP_ACTION = TURNUS_PROPERTY.concat("action");
	public static final String SP_ACTOR_CLASS = TURNUS_PROPERTY.concat("action");
	public static final String SP_TOPOLOGICAL_ORDER = TURNUS_PROPERTY.concat("tid");
	public static final String SP_READ_TOKENS = TURNUS_PROPERTY.concat("rtokens");
	public static final String SP_WRITE_TOKENS = TURNUS_PROPERTY.concat("wtokens");
	public static final String SP_READ_VARIABLES = TURNUS_PROPERTY.concat("rvariables");
	public static final String SP_WRITE_VARIABLES = TURNUS_PROPERTY.concat("wvariables");

	public static final String DP_SOURCE_ID = TURNUS_PROPERTY.concat("source-id");
	public static final String DP_TARGET_ID = TURNUS_PROPERTY.concat("target-id");
	public static final String DP_SOURCE_ACTOR = TURNUS_PROPERTY.concat("source-actor");
	public static final String DP_TARGET_ACTOR = TURNUS_PROPERTY.concat("target-actor");
	public static final String DP_SOURCE_ACTION = TURNUS_PROPERTY.concat("source-action");
	public static final String DP_TARGET_ACTION = TURNUS_PROPERTY.concat("target-action");
	public static final String DP_GUARD = TURNUS_PROPERTY.concat("guard");
	public static final String DP_PORT = TURNUS_PROPERTY.concat("port");
	public static final String DP_SOURCE_PORT = TURNUS_PROPERTY.concat("sourceport");
	public static final String DP_TARGET_PORT = TURNUS_PROPERTY.concat("targetport");
	public static final String DP_TOKENS = TURNUS_PROPERTY.concat("tokens");
	public static final String DP_VARIABLE = TURNUS_PROPERTY.concat("variables");
	public static final String DP_DIRECTION = TURNUS_PROPERTY.concat("direction");

}
