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

import static turnus.common.TurnusConstants.DB_PATH_NAME;
import static turnus.common.TurnusConstants.TRACE_PROPERTIES_FILE;
import static turnus.neo4j.trace.NeoConstants.DP_DIRECTION;
import static turnus.neo4j.trace.NeoConstants.DP_GUARD;
import static turnus.neo4j.trace.NeoConstants.DP_PORT;
import static turnus.neo4j.trace.NeoConstants.DP_SOURCE_ACTION;
import static turnus.neo4j.trace.NeoConstants.DP_SOURCE_ACTOR;
import static turnus.neo4j.trace.NeoConstants.DP_SOURCE_ID;
import static turnus.neo4j.trace.NeoConstants.DP_SOURCE_PORT;
import static turnus.neo4j.trace.NeoConstants.DP_TARGET_ACTION;
import static turnus.neo4j.trace.NeoConstants.DP_TARGET_ACTOR;
import static turnus.neo4j.trace.NeoConstants.DP_TARGET_ID;
import static turnus.neo4j.trace.NeoConstants.DP_TARGET_PORT;
import static turnus.neo4j.trace.NeoConstants.DP_TOKENS;
import static turnus.neo4j.trace.NeoConstants.DP_VARIABLE;
import static turnus.neo4j.trace.NeoConstants.LB_STEP;
import static turnus.neo4j.trace.NeoConstants.NEO4J_DEFAULT_CONF;
import static turnus.neo4j.trace.NeoConstants.SP_ACTION;
import static turnus.neo4j.trace.NeoConstants.SP_ACTOR;
import static turnus.neo4j.trace.NeoConstants.SP_ACTOR_CLASS;
import static turnus.neo4j.trace.NeoConstants.SP_ID;
import static turnus.neo4j.trace.NeoConstants.SP_READ_TOKENS;
import static turnus.neo4j.trace.NeoConstants.SP_READ_VARIABLES;
import static turnus.neo4j.trace.NeoConstants.SP_WRITE_TOKENS;
import static turnus.neo4j.trace.NeoConstants.SP_WRITE_VARIABLES;
import static turnus.neo4j.trace.NeoConstants.RelType.getRelType;
import static turnus.neo4j.trace.NeoTrace.serialize;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import turnus.common.TurnusRuntimeException;
import turnus.common.configuration.Configuration;
import turnus.common.io.Logger;
import turnus.common.util.Timer;
import turnus.model.trace.Dependency.Direction;
import turnus.model.trace.Dependency.Kind;
import turnus.model.trace.Trace;
import turnus.model.trace.TraceBuilder;
import turnus.model.trace.util.TraceProperties;

/**
 * 
 * @author Simone Casale Brunet
 *
 */
public class NeoTraceBuilder implements TraceBuilder {

	private File traceFile;
	private File dbPath;

	private BatchInserter ndb;
	private Label stepLabel;
	private TraceProperties ntd;

	public NeoTraceBuilder(File traceFile) {
		this.traceFile = traceFile;
		dbPath = new File(traceFile.getParentFile(), DB_PATH_NAME);
	}

	@Override
	public Trace build() {
		Timer timer = new Timer();
		Logger.info("Finalising trace building on the database");
		ndb.shutdown();
		Logger.debug("Trace database finalized in %ds", timer.getElapsedS());
		try {
			File dbPath = new File(ndb.getStoreDir());
			File parametesFile = new File(dbPath, TRACE_PROPERTIES_FILE);
			ntd.store(parametesFile);
			return new NeoTrace(traceFile);
		} catch (Exception e) {
			throw new TurnusRuntimeException(
					"The trace database cannot be opened after its creation from " + ndb.getStoreDir());
		}
	}

	private static void registerShutdownHook(final BatchInserter graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					graphDb.shutdown();
					Logger.debug("neo trace batch inserter has been shutdown");
				} catch (Exception e) {
					Logger.debug("neo trace batch inserter ShutdownHook error: " + e.getMessage());
				}
			}
		});
	}

	private void addDependency(long sourceId, String sourceActor, String sourceAction, long targetId,
			String targetActor, String targetAction, Kind kind, Direction direction, String guard, String variable,
			String port, String sourcePort, String targetPort, int count, Map<String, Object> attributes) {

		Map<String, Object> properties = new HashMap<>();
		properties.put(DP_SOURCE_ID, sourceId);
		properties.put(DP_SOURCE_ACTOR, sourceActor);
		properties.put(DP_SOURCE_ACTION, sourceAction);
		properties.put(DP_TARGET_ID, targetId);
		properties.put(DP_TARGET_ACTOR, targetActor);
		properties.put(DP_TARGET_ACTION, targetAction);

		switch (kind) {
		case GUARD:
			properties.put(DP_GUARD, guard);
			properties.put(DP_DIRECTION, direction.literal());
			break;
		case PORT:
			properties.put(DP_PORT, port);
			properties.put(DP_DIRECTION, direction.literal());
			break;
		case VARIABLE:
			properties.put(DP_VARIABLE, variable);
			properties.put(DP_DIRECTION, direction.literal());
			break;
		case TOKENS:
			properties.put(DP_SOURCE_PORT, sourcePort);
			properties.put(DP_TARGET_PORT, targetPort);
			properties.put(DP_TOKENS, count);
			break;
		default:
			break;
		}

		if (attributes != null && !attributes.isEmpty()) {
			if (attributes != null && !attributes.isEmpty()) {
				Map<String, Object> serialized = serialize(attributes);
				for (Entry<String, Object> e : serialized.entrySet()) {
					properties.put(e.getKey(), e.getValue());
				}
			}
		}

		ndb.createRelationship(sourceId, targetId, getRelType(kind), properties);
		ntd.addDependency();
	}

	@Override
	public void setExpectedSize(long expectedS, long expectedD) {
	}

	@Override
	public void setConfiguration(Configuration configuration) {
		Logger.info("Configuring the trace database in \"%s\"", dbPath);
		ndb = BatchInserters.inserter(dbPath.getAbsolutePath(), NEO4J_DEFAULT_CONF);
		stepLabel = DynamicLabel.label(LB_STEP);
		ndb.createDeferredSchemaIndex(stepLabel).on(SP_ACTOR).create();
		ndb.createDeferredSchemaIndex(stepLabel).on(SP_ACTION).create();
		registerShutdownHook(ndb);
		ntd = new TraceProperties();
		Logger.info("db created in \"%s\"", dbPath);
	}

	@Override
	public void addAttributes(Map<String, Object> attributes) {
		for (Entry<String, Object> e : attributes.entrySet()) {
			ntd.setAttribute(e.getKey(), e.getValue());
		}

	}

	@Override
	public void addFsmDependency(long sourceId, String sourceActor, String sourceAction, long targetId,
			String targetActor, String targetAction, Map<String, Object> attributes) {
		addDependency(sourceId, sourceActor, sourceAction, targetId, targetActor, targetAction, Kind.FSM, null, null,
				null, null, null, null, 0, attributes);

	}

	@Override
	public void addGuardDependency(long sourceId, String sourceActor, String sourceAction, long targetId,
			String targetActor, String targetAction, String guard, Direction direction,
			Map<String, Object> attributes) {
		addDependency(sourceId, sourceActor, sourceAction, targetId, targetActor, targetAction, Kind.GUARD, direction,
				guard, null, null, null, null, 0, attributes);

	}

	@Override
	public void addPortDependency(long sourceId, String sourceActor, String sourceAction, long targetId,
			String targetActor, String targetAction, String port, Direction direction, Map<String, Object> attributes) {
		addDependency(sourceId, sourceActor, sourceAction, targetId, targetActor, targetAction, Kind.PORT, direction,
				null, null, port, null, null, 0, attributes);

	}

	@Override
	public void addStep(long id, String actor, String action, String actorClass, Map<String, Integer> readTokens,
			Map<String, Integer> writeTokens, List<String> readVariables, List<String> writeVariables,
			Map<String, Object> attributes) {
		Map<String, Object> properties = new HashMap<>();
		properties.put(SP_ACTOR, actor);
		properties.put(SP_ACTION, action);
		properties.put(SP_ACTOR_CLASS, actorClass);
		properties.put(SP_ID, id);

		if (!readTokens.isEmpty()) {
			try {
				Object serialized = serialize(readTokens);
				properties.put(SP_READ_TOKENS, serialized);
			} catch (Exception e) {
				Logger.error("Read tokens of step %d cannot be serialized", id);
			}
		}
		
		if (!writeTokens.isEmpty()) {
			try {
				Object serialized = serialize(writeTokens);
				properties.put(SP_WRITE_TOKENS, serialized);
			} catch (Exception e) {
				Logger.error("Write tokens of step %d cannot be serialized", id);
			}
		}
		
		if (!readVariables.isEmpty()) {
			try {
				Object serialized = serialize(readVariables);
				properties.put(SP_READ_VARIABLES, serialized);
			} catch (Exception e) {
				Logger.error("Read variables of step %d cannot be serialized", id);
			}
		}
		
		if (!writeVariables.isEmpty()) {
			try {
				Object serialized = serialize(writeVariables);
				properties.put(SP_WRITE_VARIABLES, serialized);
			} catch (Exception e) {
				Logger.error("Write variables of step %d cannot be serialized", id);
			}
		}

		if (attributes != null && !attributes.isEmpty()) {
			Map<String, Object> serialized = serialize(attributes);
			for (Entry<String, Object> e : serialized.entrySet()) {
				properties.put(e.getKey(), e.getValue());
			}
		}

		ndb.createNode(id, properties, stepLabel);

		ntd.addStep(actor, action);
	}

	@Override
	public void addTokensDependency(long sourceId, String sourceActor, String sourceAction, long targetId,
			String targetActor, String targetAction, String sourcePort, String targetPort, int tokens,
			Map<String, Object> attributes) {
		addDependency(sourceId, sourceActor, sourceAction, targetId, targetActor, targetAction, Kind.TOKENS, null, null,
				null, null, sourcePort, targetPort, tokens, attributes);

	}

	@Override
	public void addVariableDependency(long sourceId, String sourceActor, String sourceAction, long targetId,
			String targetActor, String targetAction, String variable, Direction direction,
			Map<String, Object> attributes) {
		addDependency(sourceId, sourceActor, sourceAction, targetId, targetActor, targetAction, Kind.VARIABLE,
				direction, null, variable, null, null, null, 0, attributes);

	}

}
