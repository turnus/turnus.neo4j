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
import static turnus.common.util.StringUtils.createRandomKey;
import static turnus.neo4j.trace.NeoConstants.CACHE_DEPENDENCIES_MAP;
import static turnus.neo4j.trace.NeoConstants.CACHE_STEPS_MAP;
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
import static turnus.neo4j.trace.NeoConstants.MAX_TRANSACTIONS;
import static turnus.neo4j.trace.NeoConstants.SP_ACTION;
import static turnus.neo4j.trace.NeoConstants.SP_ACTOR;
import static turnus.neo4j.trace.NeoConstants.SP_ACTOR_CLASS;
import static turnus.neo4j.trace.NeoConstants.SP_ID;
import static turnus.neo4j.trace.NeoConstants.SP_READ_TOKENS;
import static turnus.neo4j.trace.NeoConstants.SP_READ_VARIABLES;
import static turnus.neo4j.trace.NeoConstants.SP_WRITE_TOKENS;
import static turnus.neo4j.trace.NeoConstants.SP_WRITE_VARIABLES;
import static turnus.neo4j.trace.NeoConstants.TURNUS_PROPERTY;
import static turnus.neo4j.trace.NeoConstants.RelType.FSM;
import static turnus.neo4j.trace.NeoConstants.RelType.GUARD;
import static turnus.neo4j.trace.NeoConstants.RelType.MERGED;
import static turnus.neo4j.trace.NeoConstants.RelType.PORT;
import static turnus.neo4j.trace.NeoConstants.RelType.SCHEDULER;
import static turnus.neo4j.trace.NeoConstants.RelType.TOKENS;
import static turnus.neo4j.trace.NeoConstants.RelType.UNKONW;
import static turnus.neo4j.trace.NeoConstants.RelType.VARIABLE;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import com.google.common.primitives.Longs;

import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import turnus.common.TurnusException;
import turnus.common.io.Logger;
import turnus.common.io.ProgressPrinter;
import turnus.common.util.MapUtils;
import turnus.common.util.ObjectUtils;
import turnus.common.util.Timer;
import turnus.model.trace.Dependency;
import turnus.model.trace.Dependency.Kind;
import turnus.model.trace.Step;
import turnus.model.trace.Trace;
import turnus.model.trace.util.StepsIterable;
import turnus.model.trace.util.TraceProperties;
import turnus.neo4j.trace.NeoConstants.RelType;
import turnus.neo4j.trace.NeoConstants.TopoType;

/**
 * 
 * @author Simone Casale Brunet
 *
 */
@SuppressWarnings("unchecked")
public class NeoTrace implements Trace {

	public static Object serialize(Object obj) throws TurnusException {
		if (obj instanceof Integer || obj instanceof Double || obj instanceof Boolean) {
			return obj;
		} else {
			return ObjectUtils.serialize(obj);
		}
	}

	public static Object deserialize(Object obj) throws TurnusException {
		if (obj instanceof Integer || obj instanceof Double || obj instanceof Boolean) {
			return obj;
		} else {
			return ObjectUtils.deserialize((String) obj);
		}
	}

	public static Map<String, Object> serialize(Map<String, Object> map) {
		Map<String, Object> serializedMap = new HashMap<>();
		for (Entry<String, Object> e : map.entrySet()) {
			String name = e.getKey();
			try {
				Object obj = serialize(e.getValue());
				serializedMap.put(name, obj);
			} catch (Exception ex) {
				Logger.warning("Attribute %s cannot be serialized", name);
			}
		}

		return serializedMap;

	}

	public static Map<String, Object> deserialize(Map<String, Object> map) {
		Map<String, Object> deserializedMap = new HashMap<>();
		for (Entry<String, Object> e : map.entrySet()) {
			String name = e.getKey();
			try {
				Object value = deserialize(e.getValue());
				deserializedMap.put(name, value);
			} catch (Exception ex) {
				Logger.warning("Attribute %s cannot be deserialized", name);
			}

		}
		return deserializedMap;
	}

	private class NeoDependency implements Dependency {
		private final Relationship edge;

		private NeoDependency(Relationship edge) {
			this.edge = edge;
		}

		public boolean equals(Object obj) {
			if (obj != null && obj instanceof NeoDependency) {
				NeoDependency eo = (NeoDependency) obj;
				return eo.edge.getId() == edge.getId();
			}
			return false;
		}

		@Override
		public <T> T getAttribute(String name) {
			Object o = edge.getProperty(name);
			try {
				if (o != null) {
					o = deserialize(o);
				}
			} catch (Exception e) {

			}
			return (T) o;
		}

		@Override
		public <T> T getAttribute(String name, T defaultValue) {
			T obj = getAttribute(name);
			return obj != null ? obj : defaultValue;
		}

		@Override
		public Iterable<String> getAttributeNames() {
			Set<String> attributes = new HashSet<>();
			for (String s : edge.getPropertyKeys()) {
				if (!s.startsWith(TURNUS_PROPERTY)) {
					attributes.add(s);
					noticeTransaction();
				}
			}
			return attributes;
		}

		@Override
		public int getCount() {
			return (int) edge.getProperty(DP_TOKENS);
		}

		@Override
		public Direction getDirection() {
			return Direction.getDirection((String) edge.getProperty(DP_DIRECTION));
		}

		@Override
		public String getGuard() {
			return (String) edge.getProperty(DP_GUARD);
		}

		@Override
		public Kind getKind() {
			return RelType.getKind(edge.getType());
		}

		@Override
		public String getPort() {
			return (String) edge.getProperty(DP_PORT);
		}

		@Override
		public Step getSource() {
			return getStep(getSourceId());
		}

		@Override
		public String getSourceAction() {
			return (String) edge.getProperty(DP_SOURCE_ACTION);
		}

		@Override
		public String getSourceActor() {
			return (String) edge.getProperty(DP_SOURCE_ACTOR);
		}

		@Override
		public long getSourceId() {
			return (long) edge.getProperty(DP_SOURCE_ID);
		}

		@Override
		public String getSourcePort() {
			return (String) edge.getProperty(DP_SOURCE_PORT);
		}

		@Override
		public Step getTarget() {
			return getStep(getTargetId());
		}

		@Override
		public String getTargetAction() {
			return (String) edge.getProperty(DP_TARGET_ACTION);
		}

		@Override
		public String getTargetActor() {
			return (String) edge.getProperty(DP_TARGET_ACTOR);
		}

		@Override
		public long getTargetId() {
			return (long) edge.getProperty(DP_TARGET_ID);
		}

		@Override
		public String getTargetPort() {
			return (String) edge.getProperty(DP_TARGET_PORT);
		}

		@Override
		public String getVariable() {
			return (String) edge.getProperty(DP_VARIABLE);
		}

		@Override
		public boolean hasAttribute(String name) {
			return edge.hasProperty(name);
		}

		@Override
		public int hashCode() {
			return Longs.hashCode(edge.getId());
		}

		@Override
		public boolean removeAttribute(String name) {
			if (name.startsWith(TURNUS_PROPERTY)) {
				Logger.debug("Property \"%s\" cannot be removed: structural");
				return false;
			}
			if (edge.removeProperty(name) != null) {
				noticeTransaction();
				return true;
			} else {
				return false;
			}
		}

		@Override
		public void removeAttributes() {
			for (String s : getAttributeNames()) {
				edge.removeProperty(s);
				noticeTransaction();
			}
		}

		@Override
		public void setAttribute(String name, Object value) {
			if (name.startsWith(TURNUS_PROPERTY)) {
				Logger.debug("Property \"%s\" cannot be modified: structural");
				return;
			}

			try {
				value = serialize(value);
				edge.setProperty(name, value);
				noticeTransaction();
			} catch (Exception e) {

			}

		}

		@Override
		public String toString() {
			StringBuffer b = new StringBuffer();
			b.append("[edge] ");
			b.append(edge.getId());
			b.append(" source-id=").append(getSourceId());
			b.append(" target-id=").append(getTargetId());
			b.append(" kind=").append(getKind());
			return b.toString();
		}

		@Override
		public boolean hasAttributes() {
			for (String s : edge.getPropertyKeys()) {
				if (!s.startsWith(TURNUS_PROPERTY)) {
					return true;
				}
			}
			return false;
		}

		@Override
		public Map<String, Object> getAttributes() {
			Map<String, Object> map = new HashMap<>();
			for (String s : edge.getPropertyKeys()) {
				if (!s.startsWith(TURNUS_PROPERTY)) {
					map.put(s, edge.getProperty(s));
				}
			}
			try {
				map = deserialize(map);
			} catch (Exception e) {
				map = new HashMap<>();
			}
			return map;
		}

	}

	private class NeoStep implements Step {

		private final Node node;

		private NeoStep(Node node) {
			this.node = node;
		}

		public boolean equals(Object obj) {
			if (obj != null && obj instanceof NeoStep) {
				NeoStep so = (NeoStep) obj;
				return so.node.getId() == node.getId();
			}
			return false;
		}

		@Override
		public String getAction() {
			return (String) node.getProperty(SP_ACTION);
		}

		@Override
		public String getActor() {
			return (String) node.getProperty(SP_ACTOR);
		}

		@Override
		public String getActorClass() {
			return (String) node.getProperty(SP_ACTOR_CLASS);
		}

		@Override
		public <T> T getAttribute(String name) {
			Object o = node.getProperty(name);
			try {
				if (o != null) {
					o = deserialize(o);
				}
			} catch (Exception e) {

			}
			return (T) o;
		}

		@Override
		public <T> T getAttribute(String name, T defaultValue) {
			T obj = getAttribute(name);
			return obj != null ? obj : defaultValue;
		}

		@Override
		public Iterable<String> getAttributeNames() {
			Set<String> attributes = new HashSet<>();
			for (String s : node.getPropertyKeys()) {
				if (!s.startsWith(TURNUS_PROPERTY)) {
					attributes.add(s);
					noticeTransaction();
				}
			}
			return attributes;
		}

		@Override
		public long getId() {
			return (long) node.getProperty(SP_ID);
		}

		@Override
		public Iterable<Dependency> getIncomings() {
			Set<Dependency> incomings = new HashSet<>();
			for (Relationship e : node.getRelationships(Direction.INCOMING, FSM, GUARD, MERGED, PORT, TOKENS, VARIABLE,
					SCHEDULER, UNKONW)) {
				Dependency d = depsMap.get(e);
				if (d == null) {
					d = new NeoDependency(e);
					depsMap.put(e, d);
				}
				incomings.add(d);
			}
			return incomings;
		}

		@Override
		public Iterable<Dependency> getOutgoings() {
			Set<Dependency> outgoings = new HashSet<>();
			for (Relationship e : node.getRelationships(Direction.OUTGOING, FSM, GUARD, MERGED, PORT, TOKENS, VARIABLE,
					SCHEDULER, UNKONW)) {
				Dependency d = depsMap.get(e);
				if (d == null) {
					d = new NeoDependency(e);
					depsMap.put(e, d);
				}
				outgoings.add(d);
			}
			return outgoings;
		}

		@Override
		public boolean hasAttribute(String name) {
			return node.hasProperty(name);
		}

		@Override
		public int hashCode() {
			return Longs.hashCode(node.getId());
		}

		@Override
		public boolean removeAttribute(String name) {
			if (name.startsWith(TURNUS_PROPERTY)) {
				Logger.debug("Property \"%s\" cannot be removed: structural");
				return false;
			}

			if (node.removeProperty(name) != null) {
				noticeTransaction();
				return true;
			} else {
				return false;
			}
		}

		@Override
		public void removeAttributes() {
			for (String s : getAttributeNames()) {
				node.removeProperty(s);
				noticeTransaction();
			}
		}

		@Override
		public void setAttribute(String name, Object value) {
			if (name.startsWith(TURNUS_PROPERTY)) {
				Logger.debug("Property \"%s\" cannot be modified: structural");
				return;
			}

			try {
				value = serialize(value);
				node.setProperty(name, value);
				noticeTransaction();
			} catch (Exception e) {

			}

		}

		@Override
		public String toString() {
			StringBuffer b = new StringBuffer();
			b.append("[step] ");
			b.append(node.getId());
			b.append(" actor=").append(getActor());
			b.append(" action=").append(getAction());
			return b.toString();
		}

		@Override
		public boolean hasAttributes() {
			for (String s : node.getPropertyKeys()) {
				if (!s.startsWith(TURNUS_PROPERTY)) {
					return true;
				}
			}
			return false;
		}

		@Override
		public Map<String, Object> getAttributes() {
			Map<String, Object> map = new HashMap<>();
			for (String s : node.getPropertyKeys()) {
				if (!s.startsWith(TURNUS_PROPERTY)) {
					map.put(s, node.getProperty(s));
				}
			}
			try {
				map = deserialize(map);
			} catch (Exception e) {
				map = new HashMap<>();
			}
			return map;
		}

		@Override
		public Map<String, Integer> getReadTokens() {
			if (node.hasProperty(SP_READ_TOKENS)) {
				try {
					Object map = deserialize(node.getProperty(SP_READ_TOKENS));
					return (Map<String, Integer>) map;
				} catch (Exception e) {
					Logger.error("Read tokens of step %d cannot be deserialized", getId());
				}
			}
			return new HashMap<>();
		}

		@Override
		public Map<String, Integer> getWriteTokens() {
			if (node.hasProperty(SP_WRITE_TOKENS)) {
				try {
					Object map = deserialize(node.getProperty(SP_WRITE_TOKENS));
					return (Map<String, Integer>) map;
				} catch (Exception e) {
					Logger.error("Write tokens of step %d cannot be deserialized", getId());
				}
			}
			return new HashMap<>();
		}

		@Override
		public List<String> getReadVariables() {
			if (node.hasProperty(SP_READ_VARIABLES)) {
				try {
					Object list = deserialize(node.getProperty(SP_READ_VARIABLES));
					return (List<String>) list;
				} catch (Exception e) {
					Logger.error("Read variables of step %d cannot be deserialized", getId());
				}
			}
			return new ArrayList<>();
		}

		@Override
		public List<String> getWriteVariables() {
			if (node.hasProperty(SP_WRITE_VARIABLES)) {
				try {
					Object list = deserialize(node.getProperty(SP_WRITE_VARIABLES));
					return (List<String>) list;
				} catch (Exception e) {
					Logger.error("Write variables of step %d cannot be deserialized", getId());
				}
			}
			return new ArrayList<>();
		}

	}

	private static void registerShutdownHook(final GraphDatabaseService db, final TraceProperties tData,
			final File dbPath) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					db.shutdown();
					Logger.debug("neo trace has been shutdown");
				} catch (Exception e) {
					Logger.debug("neo trace ShutdownHook error: " + e.getMessage());
				}

				try {
					File parametersFile = new File(dbPath, TRACE_PROPERTIES_FILE);
					tData.store(parametersFile);
					Logger.debug("neo trace file parameter has been stored");
				} catch (Exception e) {
					Logger.debug("neo trace file parameter cannot be stored: " + e.getMessage());
				}
			}
		});

	}

	private int cachedTransactions;
	private Map<Relationship, Dependency> depsMap = MapUtils.createCacheMap(CACHE_DEPENDENCIES_MAP);
	private GraphDatabaseService graphDb;

	private TraceProperties tData;
	private File traceFile;
	private Map<Long, NeoStep> stepsMap = MapUtils.createCacheMap(CACHE_STEPS_MAP);

	private Transaction tx;

	public NeoTrace(File traceFile) throws TurnusException {
		File dbPath = new File(traceFile.getParentFile(), DB_PATH_NAME);
		if (!dbPath.exists()) {
			throw new TurnusException("The database path does not exist");
		}

		this.traceFile = traceFile;

		try { // loading and opening the database
			Timer timer = new Timer();
			Logger.info("Opening the trace database from \"%s\"", dbPath);
			File parametesFile = new File(dbPath, TRACE_PROPERTIES_FILE);
			tData = TraceProperties.load(parametesFile);
			graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath.getAbsolutePath())
					.setConfig(GraphDatabaseSettings.cache_type, "weak")
					.setConfig(GraphDatabaseSettings.mapped_memory_page_size, "4096")
					.setConfig(GraphDatabaseSettings.string_block_size, "4096")
					.setConfig(GraphDatabaseSettings.array_block_size, "1")
					.setConfig(GraphDatabaseSettings.query_cache_size, "10000").newGraphDatabase();
			tx = graphDb.beginTx();
			Logger.debug("Opening time %d", timer.getElapsedS());
			Logger.info("The graph database is now opened and ready");
		} catch (Exception e) {
			throw new TurnusException("The database cannot be loaded", e);
		}

		registerShutdownHook(graphDb, tData, dbPath);
	}

	@Override
	public Dependency addSchedulerDependency(Step source, Step target) {
		Node srcNode = ((NeoStep) source).node;
		Node tgtNode = ((NeoStep) target).node;

		Relationship e = srcNode.createRelationshipTo(tgtNode, TopoType.TOPOLOGICAL);
		tData.addDependency();

		return new NeoDependency(e);
	}

	@Override
	public boolean areDependenciesMerged(Kind kind) {
		return false;
	}

	@Override
	public boolean close() {
		try {
			tx.success();
			graphDb.shutdown();
			return true;
		} catch (Exception e) {
			Logger.debug("Error closing the trace database: %s", e.getMessage());
		}
		return false;
	}

	private int getDepsDegree(Node node, Direction dir) {
		int c = 0;
		c += node.getDegree(FSM, dir);
		c += node.getDegree(PORT, dir);
		c += node.getDegree(GUARD, dir);
		c += node.getDegree(VARIABLE, dir);
		c += node.getDegree(TOKENS, dir);
		c += node.getDegree(SCHEDULER, dir);
		c += node.getDegree(MERGED, dir);

		return c;
	}

	@Override
	public long getSizeD() {
		return tData.getDependencies();
	}

	@Override
	public long getSizeS() {
		return tData.getSteps();
	}

	@Override
	public Step getStep(long id) {
		NeoStep step = stepsMap.get(id);
		if (step == null) {
			Node node = graphDb.getNodeById(id);
			if (node == null) {
				Logger.error("A step with id \"%d\" is not registered in this trace", id);
				return null;
			}
			step = new NeoStep(node);
			stepsMap.put(id, step);
		}
		return step;
	}

	@Override
	public Iterable<Step> getSteps(Order order) {
		if ((order == Order.INCREASING_TO || order == Order.DECREASING_TO) && !isSorted()) {
			sort();
		}

		Iterator<Step> iterator = null;

		switch (order) {
		case INCREASING_TO:
			iterator = new Iterator<Step>() {
				Node currentNode = null;
				long currentStep = 0;

				@Override
				public boolean hasNext() {
					return currentStep < tData.getSteps();
				}

				@Override
				public Step next() {
					if (currentStep == 0) {
						currentNode = graphDb.getNodeById(tData.getSourceNode());
					} else {
						Relationship r = currentNode.getSingleRelationship(TopoType.TOPOLOGICAL, Direction.OUTGOING);
						currentNode = r.getEndNode();
					}
					currentStep++;
					return getStep(currentNode.getId());
				}

				@Override
				public void remove() {
				}
			};
			break;
		case DECREASING_TO:
			iterator = new Iterator<Step>() {
				Node currentNode = null;
				long currentStep = 0;

				@Override
				public boolean hasNext() {
					return currentStep < tData.getSteps();
				}

				@Override
				public Step next() {
					if (currentStep == 0) {
						currentNode = graphDb.getNodeById(tData.getSinkNode());
					} else {
						Relationship r = currentNode.getSingleRelationship(TopoType.TOPOLOGICAL, Direction.INCOMING);
						currentNode = r.getStartNode();
					}
					currentStep++;
					return getStep(currentNode.getId());
				}

				@Override
				public void remove() {
				}
			};
			break;
		case INCREASING_ID:
			iterator = new Iterator<Step>() {
				long currentStep = 0;

				@Override
				public boolean hasNext() {
					return currentStep < tData.getSteps();
				}

				@Override
				public Step next() {
					return getStep(currentStep++);
				}

				@Override
				public void remove() {
				}
			};
			break;
		case DECREASING_ID:
			iterator = new Iterator<Step>() {
				long currentStep = tData.getSteps();

				@Override
				public boolean hasNext() {
					return currentStep > 0;
				}

				@Override
				public Step next() {
					return getStep(--currentStep);
				}

				@Override
				public void remove() {
				}
			};
			break;
		}

		return new StepsIterable(iterator);
	}

	@Override
	public Iterable<Step> getSteps(Order order, final String actor) {
		if ((order == Order.INCREASING_TO || order == Order.DECREASING_TO) && !isSorted()) {
			sort();
		}

		Iterator<Step> iterator = null;

		switch (order) {
		case INCREASING_TO:
			iterator = new Iterator<Step>() {
				long actorSteps = tData.getSteps(actor);
				Node currentNode = graphDb.getNodeById(tData.getSourceNode());
				long foundSteps = 0;

				@Override
				public boolean hasNext() {
					return foundSteps < actorSteps;
				}

				@Override
				public Step next() {
					for (;;) {
						Step step = null;
						if (currentNode.getProperty(SP_ACTOR).equals(actor)) {
							step = getStep(currentNode.getId());
							foundSteps++;
						}
						Relationship r = currentNode.getSingleRelationship(TopoType.TOPOLOGICAL, Direction.OUTGOING);
						currentNode = r.getEndNode();
						if (step != null) {
							return step;
						}
					}
				}

				@Override
				public void remove() {
				}
			};
			break;
		case DECREASING_TO:
			iterator = new Iterator<Step>() {
				long actorSteps = tData.getSteps(actor);
				Node currentNode = graphDb.getNodeById(tData.getSinkNode());
				long foundSteps = 0;

				@Override
				public boolean hasNext() {
					return foundSteps < actorSteps;
				}

				@Override
				public Step next() {
					for (;;) {
						Step step = null;
						if (currentNode.getProperty(SP_ACTOR).equals(actor)) {
							step = getStep(currentNode.getId());
							foundSteps++;
						}
						Relationship r = currentNode.getSingleRelationship(TopoType.TOPOLOGICAL, Direction.INCOMING);
						currentNode = r.getStartNode();
						if (step != null) {
							return step;
						}
					}
				}

				@Override
				public void remove() {
				}
			};
			break;
		case INCREASING_ID:
			iterator = new Iterator<Step>() {
				long actorSteps = tData.getSteps(actor);
				long currentStep = 0;
				long foundSteps = 0;

				@Override
				public boolean hasNext() {
					return foundSteps < actorSteps;
				}

				@Override
				public Step next() {
					for (;;) {
						Node node = graphDb.getNodeById(currentStep++);
						if (node.getProperty(SP_ACTOR).equals(actor)) {
							foundSteps++;
							return getStep(node.getId());
						}
					}
				}

				@Override
				public void remove() {
				}
			};
			break;
		case DECREASING_ID:
			iterator = new Iterator<Step>() {
				long actorSteps = tData.getSteps(actor);
				long currentStep = tData.getSteps();
				long foundSteps = 0;

				@Override
				public boolean hasNext() {
					return foundSteps < actorSteps;
				}

				@Override
				public Step next() {
					for (;;) {
						Node node = graphDb.getNodeById(--currentStep);
						if (node.getProperty(SP_ACTOR).equals(actor)) {
							foundSteps++;
							return getStep(node.getId());
						}
					}
				}

				@Override
				public void remove() {
				}
			};
			break;
		}

		return new StepsIterable(iterator);
	}

	@Override
	public Iterable<Step> getSteps(Order order, final String actor, final String action) {
		if ((order == Order.INCREASING_TO || order == Order.DECREASING_TO) && !isSorted()) {
			sort();
		}

		Iterator<Step> iterator = null;

		switch (order) {
		case INCREASING_TO:
			iterator = new Iterator<Step>() {
				long actorSteps = tData.getSteps(actor, action);
				Node currentNode = graphDb.getNodeById(tData.getSourceNode());
				long foundSteps = 0;

				@Override
				public boolean hasNext() {
					return foundSteps < actorSteps;
				}

				@Override
				public Step next() {
					for (;;) {
						Step step = null;
						if (currentNode.getProperty(SP_ACTOR).equals(actor)
								&& currentNode.getProperty(SP_ACTION).equals(action)) {
							step = getStep(currentNode.getId());
							foundSteps++;
						}
						Relationship r = currentNode.getSingleRelationship(TopoType.TOPOLOGICAL, Direction.OUTGOING);
						currentNode = r.getEndNode();
						if (step != null) {
							return step;
						}
					}
				}

				@Override
				public void remove() {
				}
			};
			break;
		case DECREASING_TO:
			iterator = new Iterator<Step>() {
				long actorSteps = tData.getSteps(actor, action);
				Node currentNode = graphDb.getNodeById(tData.getSinkNode());
				long foundSteps = 0;

				@Override
				public boolean hasNext() {
					return foundSteps < actorSteps;
				}

				@Override
				public Step next() {
					for (;;) {
						Step step = null;
						if (currentNode.getProperty(SP_ACTOR).equals(actor)
								&& currentNode.getProperty(SP_ACTION).equals(action)) {
							step = getStep(currentNode.getId());
							foundSteps++;
						}
						Relationship r = currentNode.getSingleRelationship(TopoType.TOPOLOGICAL, Direction.INCOMING);
						currentNode = r.getStartNode();
						if (step != null) {
							return step;
						}
					}
				}

				@Override
				public void remove() {
				}
			};
			break;
		case INCREASING_ID:
			iterator = new Iterator<Step>() {
				long actorSteps = tData.getSteps(actor, action);
				long currentStep = 0;
				long foundSteps = 0;

				@Override
				public boolean hasNext() {
					return foundSteps < actorSteps;
				}

				@Override
				public Step next() {
					for (;;) {
						Node node = graphDb.getNodeById(currentStep++);
						if (node.getProperty(SP_ACTOR).equals(actor) && node.getProperty(SP_ACTION).equals(action)) {
							foundSteps++;
							return getStep(node.getId());
						}
					}
				}

				@Override
				public void remove() {
				}
			};
			break;
		case DECREASING_ID:
			iterator = new Iterator<Step>() {
				long actorSteps = tData.getSteps(actor, action);
				long currentStep = tData.getSteps();
				long foundSteps = 0;

				@Override
				public boolean hasNext() {
					return foundSteps < actorSteps;
				}

				@Override
				public Step next() {
					for (;;) {
						Node node = graphDb.getNodeById(--currentStep);
						if (node.getProperty(SP_ACTOR).equals(actor) && node.getProperty(SP_ACTION).equals(action)) {
							foundSteps++;
							return getStep(node.getId());
						}
					}
				}

				@Override
				public void remove() {
				}
			};
			break;
		}

		return new StepsIterable(iterator);
	}

	@Override
	public boolean isSorted() {
		return tData.isSorted();
	}

	private void noticeTransaction() {
		cachedTransactions++;
		if (cachedTransactions > MAX_TRANSACTIONS) {
			Logger.debug("storing transactions");
			tx.success();
			tx = graphDb.beginTx();
			cachedTransactions = 0;
		}
	}

	@Override
	public void removeSchedulerDependencies() {
		for (long i = 0; i < tData.getSteps(); i++) {
			Node node = graphDb.getNodeById(i);
			Iterable<Relationship> toBeRemoved = node.getRelationships(Direction.INCOMING, RelType.SCHEDULER);
			for (Relationship r : toBeRemoved) {
				r.delete();
				tData.removeDependency();
			}
		}
	}

	@Override
	public void sort() {
		if (isSorted()) {
			Logger.info("The trace is already sorted");
			return;
		}

		Set<Node> sucessors = new HashSet<>();
		String REMOVED_EDGES = createRandomKey(this.getClass().getName(), "sort");

		ObjectBigArrayBigList<Node> Sn = new ObjectBigArrayBigList<Node>();
		for (long i = 0; i < tData.getSteps(); i++) {
			Node n = graphDb.getNodeById(i);
			if (getDepsDegree(n, Direction.INCOMING) == 0) {
				Sn.push(n);
			}

			Relationship r = n.getSingleRelationship(TopoType.TOPOLOGICAL, Direction.OUTGOING);
			if (r != null) {
				r.delete();
			}

		}

		ProgressPrinter progress = new ProgressPrinter("Trace topological sorting", getSizeS());
		Node lastNode = null;
		while (!Sn.isEmpty()) {
			Node n = Sn.pop();
			if (lastNode == null) {
				tData.setSourceNode(n.getId());
			} else {
				lastNode.createRelationshipTo(n, TopoType.TOPOLOGICAL);
			}
			lastNode = n;

			// orderedSteps.push(s);
			// System.out.println("sorted: " + s);
			progress.increment();

			sucessors.clear();
			for (Relationship outgoing : n.getRelationships(Direction.OUTGOING, FSM, GUARD, MERGED, PORT, TOKENS,
					VARIABLE, SCHEDULER, UNKONW)) {
				outgoing.setProperty(REMOVED_EDGES, true);
				sucessors.add(outgoing.getEndNode());
			}

			// check if all edges of the sucessors are sorted
			for (Node sucessor : sucessors) {
				boolean insertInS = true;
				for (Relationship in : sucessor.getRelationships(Direction.INCOMING, FSM, GUARD, MERGED, PORT, TOKENS,
						VARIABLE, SCHEDULER, UNKONW)) {
					if (!in.hasProperty(REMOVED_EDGES)) {
						insertInS = false;
						break;
					}
				}
				if (insertInS) {
					Sn.push(sucessor);
				}
			}
		}

		tData.setSinkNode(lastNode.getId());
		tData.setSorted();
		progress.finish();

	}

	@Override
	public File getFile() {
		return traceFile;
	}

}
