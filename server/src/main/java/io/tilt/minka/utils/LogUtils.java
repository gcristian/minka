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

import java.nio.charset.Charset;
import java.util.Random;

import org.springframework.util.StreamUtils;

import io.tilt.minka.domain.ShardID;

/**
 * http://boschista.deviantart.com/journal/Cool-ASCII-Symbols-214218618
 * https://www.xatworld.com/nicks/letrasycaracteresxat.html
 */
public class LogUtils {

	public final static String logo = randomSaluteFromFile();
	
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

	public static String titleLine(final char ch, final String title, Object...params) {
		return titleLine(ch, String.format(title, params));
	}
	public static String titleLine(final String title) {
		return titleLine(GROSS_CHAR, title);
	}
	public static String titleLine(final char ch, final String title) {
		int dots = LARGE - title.length() - 2;
		StringBuilder line = new StringBuilder();
		grossLine(ch, dots, line);
		line.append(" ").append(title).append(" ");
		grossLine(ch, dots, line);
		return line.toString();
	}
	private static String randomSaluteFromFile() {
		try {
			final Random rnd = new Random();
			final int num = rnd.nextInt(2);
			return StreamUtils.copyToString(LogUtils.class.getResourceAsStream("salutation" + num + 
					 ".txt"), Charset.forName("utf-8"));
		} catch (Exception e) {
		}
		return "";
	}

	private static void grossLine(final char ch, int dots, StringBuilder line) {
		for (int i = 0; i < (dots / 2); i++)
			line.append(ch);
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
		grossLine(GROSS_CHAR, LARGE * 2, sb);
		sb.append(nl).append(nl);
		sb.append(logo);
		sb.append("    Distributing duties since 1438, where no follower has ever got").append(nl).append(nl);
		sb.append("    Service: ").append(serviceName).append(nl);
		sb.append("    ShardID: ").append(id).append(nl).append(nl);

		sb.append(END_LINE);
		return sb.toString();
	}

}
