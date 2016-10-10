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
package io.tilt.minka.utils;

import io.tilt.minka.domain.ShardID;

/**
 * http://boschista.deviantart.com/journal/Cool-ASCII-Symbols-214218618
 * https://www.xatworld.com/nicks/letrasycaracteresxat.html
 */
public class LogUtils {

	public final static String logo = "\n   ooo        ooooo  o8o              oooo      "
			+ "          \n   `88.       .888'  `\"'              `888                \n    888b"
			+ "     d'888  oooo  ooo. .oo.    888  oooo   .oooo. \n    8 Y88. .P  888  `888  "
			+ "`888P\"Y88b   888 .8P'   `P  )88b\n    8  `888'   888   888   888   888   888888.     .oP\"888\n"
			+ "    8    Y     888   888   888   888   888 `88b.  d8(  888\n   o8o        "
			+ "o888o o888o o888o o888o o888o o888o `Y888\"\"8o\n\n";

	public final static char OK = '√';
	public final static char CRASH = '⚡';
	public final static char HYPHEN_CHAR = '—';
	public final static char GROSS_CHAR = '▬';
	public final static char HB_CHAR = '♥';
	public final static char BALANCED_CHAR = '☯';
	public final static char HEALTH_UP = '▲';
	public final static char HEALTH_DOWN = '▽';
	public final static char SPECIAL = '❇';

	private static int LARGE = 120;

	public static String END_LINE = endLine();

	public static String titleLine(final String title) {
		int dots = LARGE - title.length() - 2;
		StringBuilder line = new StringBuilder();
		grossLine(dots, line);
		line.append(" ").append(title).append(" ");
		grossLine(dots, line);
		return line.toString();
	}

	private static void grossLine(int dots, StringBuilder line) {
		for (int i = 0; i < (dots / 2); i++)
			line.append(GROSS_CHAR);
	}

	private static String endLine() {
		StringBuilder line = new StringBuilder();
		for (int i = 0; i < LARGE; i++)
			line.append(HYPHEN_CHAR);
		return line.toString();
	}

	public static String getGreetings(final ShardID id, final String serviceName) {
		final String nl = System.getProperty("line.separator");
		StringBuilder sb = new StringBuilder(nl);
		grossLine(LARGE * 2, sb);
		sb.append(nl).append(nl);
		sb.append(logo);
		sb.append("    Distributing duties since 1438, where no follower has ever got").append(nl).append(nl);
		sb.append("    Service: ").append(serviceName).append(nl);
		sb.append("    ShardID: ").append(id).append(nl).append(nl);

		sb.append(END_LINE);
		return sb.toString();
	}

}
