package io.tilt.minka.sampler;

import static com.google.common.collect.Sets.newHashSet;

import java.net.NetworkInterface;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

import io.tilt.minka.ServerWhitness;
import io.tilt.minka.api.Client;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.EventMapper;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.Reply;
import io.tilt.minka.api.Server;
import io.tilt.minka.shard.TCPShardIdentifier;

public class CmdLineApp {

	public enum Quest {
		namespace("cluster Namespace (to reach other nodes)"),
		tag("node Tag (for tagging your node within the cluster)"),
		//address("node address (your public LAN ip)"),
		zk("Zookeeper (ask Cristian)"),
		address("Address (confirm if is a public address IP)")
		;
		private String title;

		Quest(String v) {
			this.title = v;
		}
		public String getTitle() {
			return title;
		}
	}
	
	final Pallet p = Pallet.builder("G1").build();
	final Set<Pallet> pallets = newHashSet(p);
	
	private Server build(final Map<Quest, String> quest) {
		
		final Config ownConfig = new Config();
		
		ownConfig.getBroker().setHostAndPort(quest.get(Quest.address), 5000);
		ownConfig.getBootstrap().setServerTag(quest.get(Quest.tag));
		ownConfig.getBootstrap().setNamespace(quest.get(Quest.namespace));
		ownConfig.getBootstrap().setZookeeperHostPort(quest.get(Quest.zk));
		
		ownConfig.getBroker().setEnablePortFallback(true);
		ownConfig.getBootstrap().setBeatUnitMs(100);
		ownConfig.getBootstrap().setDropVMLimit(true);
		ownConfig.getBootstrap().setEnableCoreDump(true);
		ownConfig.getBootstrap().setCoreDumpFilepath("/tmp/");

		final Set<Duty> everCaptured = new HashSet<>();
		final Set<Duty> everReleased = new HashSet<>();
		final Set<Duty> current = new HashSet<>();
		final Object[] o = {null};
		final Server server = new Server(ownConfig);
		final EventMapper mapper = server.getEventMapper();
		
		for (Pallet p: pallets) {
			mapper.setCapacity(p, 100);
		}
		
		mapper.onPalletLoad(() -> pallets)
			.onActivation(()->{})
			.onDeactivation(()->{})
			.onTransfer((a,b)->{})
			.onLoad(()-> Collections.emptySet())
			.onPalletRelease(p->o[0]=p)
			.onPalletCapture(p->o[0]=p)
			.onCapture(d-> { 
				everCaptured.addAll(d);
				current.addAll(d);
			})
			.onRelease(d-> {
				everReleased.addAll(d);
				current.removeAll(d);
			})
			.done();
		
		return server;
	}
	
	public static void main(String[] args) {
		new CmdLineApp().run();	
	}

	private void run() {
		Server server = null;
		try (Scanner scan = new Scanner(System.in)) {
			final Map<Quest, String> quest = readParameters(scan);
			server = build(quest);
			readDuties(server.getClient(), scan, quest);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (server!=null) {
				server.shutdown();
			}
			System.out.println("good bye !");
		}
	}

	private Map<Quest, String> readParameters(Scanner scan) {
		
		final Map<Quest, String> suggest = new HashMap<>();
		suggest.put(Quest.namespace, "olxdemo");
		suggest.put(Quest.tag, System.getProperty("user.name"));
		suggest.put(Quest.zk, "localhost:2181");
		suggest.put(Quest.address, TCPShardIdentifier.findLANAddress().getHostAddress());
		
		final Map<Quest, String> quest = new HashMap<>();
		
		System.out.println("Welcome ! First some answers (always type quit to exit)");
		System.out.println("==========================================================");
		for(Quest q: Quest.values()) {
			while (!Thread.interrupted() && true) {
				System.out.println("Enter " + q.getTitle() + ": ");
				System.out.print("\t( " + suggest.get(q) + " ? ) ");
				if (scan.hasNextLine()) {
					String line = scan.nextLine();
					if ((line==null || line.length()<1)&& suggest.containsKey(q)) {
						quest.put(q, suggest.get(q));
						break;
					} else if (line.equalsIgnoreCase("quit")) {
						break;
					} else {
						quest.put(q, line);
						break;
					}
				} else {
					System.out.println("no input !");
				}
			}
		}
		return quest;
	}

	private void readDuties(Client  client, Scanner scan, final Map<Quest, String> quest) {
		System.out.println("Now enter tasks");
		final String remem = "(type quit to finish, or \n alphanumeric, preffer propercase, \n 32 chars max, no spaces, "
				+ "\n mnemotecnic, like 'paralelo55' or 'chapulin22' "
				+ "[! task-id] to delete a task)" ;
		
		System.out.println(remem);
		
		while (scan.hasNextLine() && !Thread.interrupted()) {
			String task = scan.nextLine();
			if (task==null || task.length()<1) {
				System.out.println(remem);
				continue;
			} else if (task.equalsIgnoreCase("quit")) {
				break;
			}
			
			task = StringUtils.remove(StringUtils.abbreviate(task, 32).trim(), " ");
			final boolean delete = task.startsWith("!");
			if (delete) {
				task=task.substring(1, task.length());
			}
			Duty d = Duty.builder(quest.get(Quest.tag) + "-" + task, p.getId()).with(1).build();
			if (delete) {
				final Reply res = client.remove(d);
				System.out.println(res.toMessage());
			} else {
				final Reply res = client.add(d);
				System.out.println(res.toMessage());
			}
		}
	}
	
}
