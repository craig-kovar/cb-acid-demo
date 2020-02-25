package scratch;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.TransactionResult;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.deferred.TransactionSerializedContext;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.IllegalDocumentState;
import com.couchbase.transactions.log.LogDefer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

//import io.opentracing.Tracer;

class InsufficientFunds extends RuntimeException {}
class CustomerNotFound extends RuntimeException {}

public class Scratch {

	private static Logger logger = LoggerFactory.getLogger(Scratch.class);
	
	static final String USER = "Administrator";
	static final String USER_PASS = "password";
	//static final String CB_IP = "aciddemo-0000.aciddemo.se-couchbasedemos.com";
	static final String CB_IP = "localhost";
	static final String CERT_PATH = "/Users/craigkovar/Desktop/Demos/Minishift/eks_tool/work/aciddemo/easy-rsa/easyrsa3/pki/issued/ca.cert";
	
	public static void main(String[] args) {
		
		//You only need to use on of the below connection methods.
		//The default is the simplest and can be used in most cases such as local deployments
		//The custom connection allows greater control but requires more configuration upfront
		
		//Default Connection Information
		Cluster cluster = Cluster.connect(CB_IP, USER, USER_PASS);
		
		//Custom connection information
		/*
		ClusterEnvironment env = ClusterEnvironment.builder()
				.securityConfig(SecurityConfig.enableTls(true)
						.trustCertificate(Paths.get(CERT_PATH)
								))
				.ioConfig(IoConfig.networkResolution(NetworkResolution.EXTERNAL))
				.timeoutConfig(
						TimeoutConfig
						.connectTimeout(Duration.ofSeconds(5))
						.kvTimeout(Duration.ofSeconds(5))
						.queryTimeout(Duration.ofSeconds(10))
					)
				.build();
		
		//Connect to the cluster
		Cluster cluster = Cluster.connect(CB_IP, 
				ClusterOptions.clusterOptions(USER, USER_PASS)
				.environment(env)
			);
		*/
		
		Bucket bucketCustomers = cluster.bucket("customers");
		Collection customers = bucketCustomers.defaultCollection();
		Bucket bucketTxn = cluster.bucket("transfer");
		Collection transfer = bucketTxn.defaultCollection();
		cluster.waitUntilReady(Duration.ofSeconds(10));
		
		// Create the single Transactions object
		Transactions txn = Transactions.create(cluster, TransactionConfigBuilder.create()
				//If running this locally with only 1 node in the cluster this needs to be set to NONE
				.durabilityLevel(TransactionDurabilityLevel.NONE)
				.expirationTime(Duration.ofSeconds(15))
		        // The configuration can be altered here, but in most cases the defaults are fine.
		        .build());
		
		//Main work here
		pause("Bucket opened setting up customers here - Hit any key to continue");
		setUp(customers);
		while(pause("Enter 'Quit' to exit, any other value to run transaction")) {
			doTxn(customers, transfer, txn, 30, true);
		}
		
		//Disconnect and close connection
		cluster.disconnect();
		//env.shutdown();
	}

	//Simple method to run the transaction
	private static void doTxn(Collection customers, Collection transfer, Transactions txn, 
			Integer amount, boolean doPause) {
		logger.info("Running transaction with amount: " + amount);
		
		AtomicReference<Integer> attempt = new AtomicReference<Integer>();
		attempt.set(0);

        try {

            // Supply transactional logic inside a lambda - any required retries are handled for you
            txn.run(ctx -> {

            	attempt.set(attempt.get()+1);
            	logger.info("TXN - Attempt : " + attempt.get());
            	
            	logger.info("Getting documents involved in txn");
                // get will error if record does not exist, use getOptional if record may not exist
            	TransactionGetResult source = ctx.get(customers, "user::andy");
            	TransactionGetResult target = ctx.get(customers, "user::sue");
            	
            	JsonObject sourceJO = source.contentAsObject();
            	JsonObject targetJO = target.contentAsObject();

                int sourceBalance = sourceJO.getInt("amount");
                int targetBalance = targetJO.getInt("amount");

                // Create a record of the transfer
                JsonObject transferRecord = JsonObject.create()
                        .put("from", "andy example")
                        .put("to", "sue example")
                        .put("amount", amount)
                        .put("type", "Transfer");

                ctx.insert(transfer, UUID.randomUUID().toString(), transferRecord);
                logger.info("Current Balance: " + sourceBalance + " -- " + targetBalance);
                
                if (sourceBalance >= amount) {

                    sourceJO.put("amount", sourceBalance - amount);
                    targetJO.put("amount", targetBalance + amount);
                    
                    ctx.replace(source, sourceJO);
                    ctx.replace(target, targetJO);
                    
                    if (doPause) {
                    	pause("Pausing txn - enter any value to continue");
                    }
                }
                else {
                    // Rollback is automatic on a thrown exception.  This will also cause the transaction to fail
                    // with a TransactionFailed containing this InsufficientFunds as the getCause() - see below.
                    throw new InsufficientFunds();
                }

                // If we reach here, commit is automatic.
                logger.info("In transaction - about to commit");
                ctx.commit(); // can also, and optionally, explicitly commit
            });


        } catch (TransactionFailed err) {

            if (err.getCause() instanceof InsufficientFunds) {
            	logger.info("Insufficient Funds to transfer, handling gracefully");
                //throw (RuntimeException) err.getCause(); // propagate up
            }  else {
                // Unexpected error - log for human review
                // This per-txn log allows the app to only log failures
                System.err.println("Transaction " + err.result().transactionId() + " failed: " + err.getCause());
                for (LogDefer e : err.result().log().logs()) {
                	System.err.println(e);
                }
            }
        }
		
	}
	
	//Simple method to create 3 users with different starting amounts.  This does not use transactions
	private static void setUp(Collection customers) {
		//Build User Andy
		JsonObject andy = JsonObject.create();
		andy.put("name", "andy example");
		andy.put("amount", 100);
		
		JsonArray address = JsonArray.create();
		JsonObject homeAddr = JsonObject.create();
		homeAddr.put("stline1", "123 Main Street");
		homeAddr.put("city", "Chicago");
		homeAddr.put("state", "IL");
		homeAddr.put("zipcode", "60606");
		homeAddr.put("addr_type", "home address");
		address.add(homeAddr);
		
		JsonObject businessAddr = JsonObject.create();
		businessAddr.put("stline1", "456 Other Street");
		businessAddr.put("city", "St. Charles");
		businessAddr.put("state", "IL");
		businessAddr.put("zipcode", "60174");
		businessAddr.put("addr_type", "business address");
		address.add(businessAddr);
		
		andy.put("addresses", address);
		
		//Build User Sue
		JsonObject sue = JsonObject.create();
		sue.put("name", "sue example");
		sue.put("amount", 100);
		
		JsonArray sueAddress = JsonArray.create();
		JsonObject sueHomeAddr = JsonObject.create();
		sueHomeAddr.put("stline1", "1682 Hoyne Ave");
		sueHomeAddr.put("city", "Chicago");
		sueHomeAddr.put("state", "IL");
		sueHomeAddr.put("zipcode", "60645");
		sueHomeAddr.put("addr_type", "home address");
		sueAddress.add(sueHomeAddr);
		
		JsonObject sueBusinessAddr = JsonObject.create();
		sueBusinessAddr.put("stline1", "444 Michigan Ave Street");
		sueBusinessAddr.put("city", "Chicago");
		sueBusinessAddr.put("state", "IL");
		sueBusinessAddr.put("zipcode", "60606");
		sueBusinessAddr.put("addr_type", "business address");
		sueAddress.add(sueBusinessAddr);
		
		sue.put("addresses", sueAddress);
		
		//Ryan
		JsonObject ryan = JsonObject.create();
		ryan.put("name", "ryan example");
		ryan.put("amount", 20);
		
		JsonArray ryanAddress = JsonArray.create();
		JsonObject ryanHomeAddr = JsonObject.create();
		ryanHomeAddr.put("stline1", "1682 Hoyne Ave");
		ryanHomeAddr.put("city", "Chicago");
		ryanHomeAddr.put("state", "IL");
		ryanHomeAddr.put("zipcode", "60645");
		ryanHomeAddr.put("addr_type", "home address");
		ryanAddress.add(ryanHomeAddr);
		
		JsonObject ryanBusinessAddr = JsonObject.create();
		ryanBusinessAddr.put("stline1", "444 Michigan Ave Street");
		ryanBusinessAddr.put("city", "Chicago");
		ryanBusinessAddr.put("state", "IL");
		ryanBusinessAddr.put("zipcode", "60606");
		ryanBusinessAddr.put("addr_type", "business address");
		ryanAddress.add(ryanBusinessAddr);
		
		ryan.put("addresses", ryanAddress);
		
		customers.upsert("user::andy", andy);
		customers.upsert("user::sue", sue);
		customers.upsert("user::ryan", ryan);
	} 
	
	//Simple pause method to wait until user input
	private static boolean pause(String s) {
		System.out.println(s);
		Scanner in = new Scanner(System.in);
        String inp = in.nextLine();	
        
        if ("Quit".equalsIgnoreCase(inp))
        	return false;
        
        return true;
	}
	
	//Customer connection information
	/*
	ClusterEnvironment env = ClusterEnvironment.builder()
			.securityConfig(SecurityConfig.enableTls(true)
					.trustCertificate(Paths.get(CERT_PATH)
							))
			//.ioConfig(ioEnv)
			.timeoutConfig(
					TimeoutConfig
					.connectTimeout(Duration.ofSeconds(5))
					.kvTimeout(Duration.ofSeconds(5))
					.queryTimeout(Duration.ofSeconds(10))
				)
			.build();
	
	//Connect to the cluster
	Cluster cluster = Cluster.connect(CB_IP, 
			ClusterOptions.clusterOptions("Administrator", "password")
			.environment(env)
		);
	*/
}
