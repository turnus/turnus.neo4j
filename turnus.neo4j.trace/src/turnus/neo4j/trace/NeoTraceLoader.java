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
import static turnus.common.TurnusConstants.DEFAULT_REMOVE_TEMP_TRACE_FILES;
import static turnus.common.TurnusOptions.CONFIG_REMOVE_TEMP_TRACE_FILES;

import java.io.File;

import turnus.common.TurnusException;
import turnus.common.configuration.Configuration;
import turnus.common.io.Logger;
import turnus.common.util.FileUtils;
import turnus.model.trace.Trace;
import turnus.model.trace.TraceLoader;
import turnus.model.trace.io.XmlTraceReader;

/**
 * 
 * @author Simone Casale Brunet
 *
 */
public class NeoTraceLoader implements TraceLoader {

	@Override
	public Trace load(File traceFile, Configuration configuration) throws TurnusException {

		File dbPath = new File(traceFile.getParentFile(), DB_PATH_NAME);
		if (dbPath.exists()
				&& !configuration.getValue(CONFIG_REMOVE_TEMP_TRACE_FILES, DEFAULT_REMOVE_TEMP_TRACE_FILES)) {
			try {
				Trace trace = new NeoTrace(traceFile);
				return trace;
			} catch (Exception e) {
				Logger.info("The execution trace database should be reloaded");
				Logger.debug(e.getMessage());
			}
		}

		// clean the directory
		if (dbPath.exists()) {
			try {
				Logger.info("Cleaning the directory %s. All the data will be deleted", dbPath);
				FileUtils.deleteDirectory(dbPath);
			} catch (Exception e) {
				Logger.warning("The database directory \"%s\" has not been completely cleaned", dbPath);
				Logger.debug(e.getMessage());
			}
		} else {
			FileUtils.createDirectory(dbPath);
		}

		NeoTraceBuilder builder = new NeoTraceBuilder(traceFile);
		builder.setConfiguration(configuration);
		new XmlTraceReader(builder, traceFile).load();
		return builder.build();
	}

}
