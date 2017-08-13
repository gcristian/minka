package io.tilt.minka.domain;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Minka;
import io.tilt.minka.api.MinkaClient;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.utils.LogUtils;

/**
 * An example of an Application loading entities to a locally created Minka server
 * 
 * @author Cristian Gonzalez
 * @since Nov 9, 2016
 */
public class SimpleClientApplication {

	private static final Logger logger = LoggerFactory.getLogger(SimpleClientApplication.class);

	// TreeSet() to enter them in the default minka order: comparator by id   
	private Set<Duty<String>> runningDuties = new TreeSet<>();

	private Minka<String, String> server;

	private String shardId = "{NN}";
	
	public SimpleClientApplication() throws Exception {
		super();
		this.runningDuties = new HashSet<>();
	}
	
	public static interface EntityProvider {
	    Set<Duty<String>> loadDuties();
	    Set<Pallet<String>> loadPallets();
	    double loadShardCapacity(Pallet<String> pallet, Set<Duty<String>> allDuties, String shardIdentifier);	    
	}
	
	public void close() {
	    if (this.server!=null) {
	        this.server.destroy();
	    }
	}
	
	public void start(final EntityProvider loader) throws Exception {
	    java.util.Objects.requireNonNull(loader);
	    
	    final Set<Duty<String>> duties = new TreeSet<>(loader.loadDuties());
	    final Set<Pallet<String>> pallets = new TreeSet<>(loader.loadPallets());

	    logger.info("{} Loading {} duties: {}", shardId, duties.size(), toStringGroupByPallet(duties));
		
		server = new Minka<>();
		server.onPalletLoad(() -> pallets);
		server.onDutyLoad(()-> duties);
		
		server.onDutyCapture((final Set<Duty<String>> t) -> {
			logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "taking"));
			logger.info("{} # {}+ ({})", shardId, t.size(), toStringIds(t));
			runningDuties.addAll(t);
		});
		server.onPalletCapture((p)->logger.info("Taking pallet: {}", p.toString()));
		
		server.onDutyRelease((final Set<Duty<String>> entities)-> {
			logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "releasing"));
			logger.info("{} # -{} ({})", shardId, entities.size(), toStringIds(entities));
			runningDuties.removeAll(entities);
		});
		server.onPalletRelease((p)->logger.info("Releasing pallet: {}", p.toString()));
		
		server.onDutyReport(()-> {
			final long now = System.currentTimeMillis();
			if (timeToLog(now)) {
				final MinkaClient<String, String> client = server.getClient();
				lastPrint = now;
				lastSize = runningDuties.size();
				logger.info(LogUtils.titleLine(LogUtils.HYPHEN_CHAR, "running"));
				if (logger.isDebugEnabled()) {
					logger.debug("{} {{}} # {} ({})", shardId, client != null ? client.isCurrentLeader() ? 
							"leader" : "follower" : "?", this.runningDuties.size(), toStringGroupByPallet(runningDuties));
				} else {
					logger.info("{} {{}} # {} ", shardId, client != null ? client.isCurrentLeader() ? 
							"leader" : "follower" : "?", this.runningDuties.size());
				}
			}
			return this.runningDuties;
		});
		
		for (final Pallet<String> pallet: pallets) {
			server.setCapacity(pallet, loader.loadShardCapacity(pallet, duties, server.getClient().getShardIdentity()));			
		}
		
		server.onDutyUpdate(d->{});
		server.onActivation(()-> {
			logger.info("activating");
			this.shardId = server.getClient().getShardIdentity();
		});
		server.onDeactivation(()->logger.info("de-activating"));
		server.onDutyUpdate(d->logger.info("receiving update: {}", d));
		server.onDutyTransfer((d, e)->logger.info("receiving transfer: {}", d));
		server.load();
	}
	
	public MinkaClient<String, String> getClient() {
	    if (this.server!=null) {
	        return this.server.getClient();
	    }
	    return null;
	}
	
	private boolean timeToLog(long now) {
		return lastSize != runningDuties.size() || now - lastPrint > 60000 * 1;
	}


	private static void sleep(final int mins) throws InterruptedException {
		for (int i = 0; i < mins; i++) {
			Thread.sleep(60 * 1000l);
			for (int j = 0; j < 200000; j++)
				;
		}
	}
	
	private String toStringGroupByPallet(Set<Duty<String>> entities) {
		final StringBuilder sb = new StringBuilder();
		entities.stream().collect(Collectors.groupingBy(t -> t.getPalletId())).forEach((palletId, duties) -> {
			sb.append(" p").append(palletId).append("->").append(toStringIds(duties)).append(", ");
		});
		entities.forEach(e -> sb.append("p" + e.getPalletId() + "d" + e.getId() + ", "));
		return sb.toString();
	}

	private static String toStringIds(Collection<Duty<String>> entities) {
		final StringBuilder sb = new StringBuilder();
		entities.forEach(e -> sb.append("p" + e.getPalletId() + "d" + e.getId() + ", "));
		return sb.toString();
	}

	int lastSize;
	long lastPrint;


}