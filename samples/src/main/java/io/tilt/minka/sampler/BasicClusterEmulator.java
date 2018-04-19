/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.tilt.minka.sampler;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.tilt.minka.api.Config;

public class BasicClusterEmulator {

	public static void main(String[] args) throws Exception {

		final String filepath = "/var/wd/dev/repos/minka/samples/dataset-1pallet-fb-1duty";

		
		final BasicAppEmulator zero = emulatedApp(filepath, 9000);
		
		sleep(SECONDS.toMillis(25));

		final BasicAppEmulator one = emulatedApp(filepath, 9001);
		final BasicAppEmulator two = emulatedApp(filepath, 9002);

		sleep(MINUTES.toMillis(2));

		/*final BasicAppEmulator three = emulatedApp(filepath, 9003);
		sleep(SECONDS.toMillis(30));

		two.getServer().destroy();

		sleep(SECONDS.toMillis(30));
		*/
		// leader change
		//one.getServer().destroy();

		////////////////////

		sleep(TimeUnit.HOURS.toMillis(1));

	}

	private static BasicAppEmulator emulatedApp(final String filepath, final int port) 
			throws FileNotFoundException, IOException, Exception {

		// Start the Application which starts a Minka server and loads duties and
		// pallets from a sampler

		final Config config = new Config("localhost:2181", "192.168.0.102:" + port);
		final BasicAppEmulator app = new BasicAppEmulator(config);
		app.launch(FileDatasetEmulator.fromFile(filepath));
		return app;
	}

}