package inc.future.redispq;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import lombok.AllArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

@AllArgsConstructor
public class RedisPriorityQueue implements Queue {
    private static final Logger LOGGER = LogManager.getLogger(RedisPriorityQueue.class);

    private static final int DEFAULT_PORT = 6379;

    private static final String DDB_HASH_KEY_PRIORITY = "priority";
    private static final String DDB_RANGE_KEY_UUID = "uuid";
    private static final String DDB_PAYLOAD_ATTR = "payload";
    private static final String IS_PEEK = "1";
    private static final String IS_POLL = "0";

    private static final String WRONG_DATA_TYPE = "Object is not Prioritizable";

    private static final String ADD_SCRIPT = "https://raw.githubusercontent.com/TusharRakheja/RedisPQScripts/master/add.lua";
    private static final String REMOVE_SCRIPT = "https://raw.githubusercontent.com/TusharRakheja/RedisPQScripts/master/remove.lua";

    private final String appName;

    private final ArrayList<Jedis> jedis;
    private final Table ddbTable;

    private final String addScriptSHA;
    private final String removeScriptSHA;

    private int size;

    public RedisPriorityQueue(
            final String appName, final AWSCredentialsProvider dynamoDBCredentialsProvider, final Regions region,
            final String hostname, String password
    ) throws IOException {
        this(appName, dynamoDBCredentialsProvider, region, hostname, DEFAULT_PORT, password);
    }

    public RedisPriorityQueue(
            final String appName, final AWSCredentialsProvider dynamoDBCredentialsProvider, final Regions region,
            final String hostname, final int port, final String password
    ) throws IOException {
        this.appName = appName;

        Jedis newJedis = new Jedis(hostname, port);
        if (password != null) {
            newJedis.auth(password);
        }
        this.jedis = new ArrayList<>(Collections.singleton(newJedis));
        DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.standard()
                .withCredentials(dynamoDBCredentialsProvider)
                .withRegion(region)
                .build()
        );
        this.ddbTable = dynamoDB.getTable(appName);
        this.addScriptSHA = this.loadScript(ADD_SCRIPT);
        this.removeScriptSHA = this.loadScript(REMOVE_SCRIPT);
        this.size = 0;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean isEmpty() {
        return this.size == 0;
    }

    @Override
    public boolean contains(Object o) {
        assert o instanceof Prioritizable : WRONG_DATA_TYPE;
        Prioritizable item = (Prioritizable) o;
        return this.ddbTable.getItem(DDB_HASH_KEY_PRIORITY, item.getPriority(), DDB_RANGE_KEY_UUID, item.getUUID()) != null;
    }

    @Override
    public boolean add(Object o) {
        assert o instanceof Prioritizable : WRONG_DATA_TYPE;
        Prioritizable item = (Prioritizable) o;

        Item ddbItem = new Item()
                .withPrimaryKey(DDB_HASH_KEY_PRIORITY, item.getPriority(), DDB_RANGE_KEY_UUID, item.getUUID())
                .withString(DDB_PAYLOAD_ATTR, item.getPayload());
        PutItemSpec putItemSpec = new PutItemSpec()
                .withItem(ddbItem)
                .withConditionExpression("attribute_not_exists(#hk) and attribute_not_exists(#rk)")
                .withNameMap(Map.of("#hk", DDB_HASH_KEY_PRIORITY, "#rk", DDB_RANGE_KEY_UUID));

        try {
            this.ddbTable.putItem(putItemSpec);
        } catch (ConditionalCheckFailedException e) {
            LOGGER.info("The item you tried to add already exists!");
            return false;
        }
        this.getRandomJedis().evalsha(this.addScriptSHA, 2, this.appName, Long.toString(item.getPriority()));

        incrementSize();
        return true;
    }

    private synchronized void incrementSize() {
        this.size++;
    }

    private synchronized void decrementSize() {
        this.size--;
    }

    @Override
    public Prioritizable remove() {
        Prioritizable p = this.poll();
        if (p == null) {
            throw new NoSuchElementException();
        }
        return p;
    }

    @Override
    public Prioritizable poll() {
        return this.get(IS_POLL);
    }

    @Override
    public Prioritizable peek() {
        return this.get(IS_PEEK);
    }

    @Override
    public Object element() {
        Prioritizable p = this.peek();
        if (p == null) {
            throw new NoSuchElementException();
        }
        return p;
    }

    private String loadScript(final String scriptAddress) throws IOException {
        URL url = new URL(scriptAddress);
        Scanner scanner = new Scanner(url.openStream());
        StringBuilder scriptBuilder = new StringBuilder();
        while (scanner.hasNextLine()) {
            scriptBuilder.append(scanner.nextLine());
            scriptBuilder.append('\n');
        }
        return this.getRandomJedis().scriptLoad(scriptBuilder.toString());
    }

    private Prioritizable get(String peekOrPoll) {
        if (isEmpty()) {
            return null;
        }
        Jedis jedis = this.getRandomJedis();
        Object priorityObj = jedis.evalsha(this.removeScriptSHA, 2, this.appName, peekOrPoll);
        if (priorityObj == null) {
            return null;
        }
        long priority = Long.parseLong((String) priorityObj);
        QuerySpec querySpec = new QuerySpec()
                .withHashKey(DDB_HASH_KEY_PRIORITY, priority)
                .withMaxResultSize(1);
        for (Page<Item, QueryOutcome> page : this.ddbTable.query(querySpec).pages()) {
            for (Item item : page) {
                String uuid = (String) item.get(DDB_RANGE_KEY_UUID);
                try {
                    return new Prioritizable() {
                        @Override
                        public Long getPriority() {
                            return priority;
                        }
                        @Override
                        public String getUUID() {
                            return uuid;
                        }
                        @Override
                        public String getPayload() {
                            return (String) item.get(DDB_PAYLOAD_ATTR);
                        }
                    };
                } finally {
                    if (peekOrPoll.equals(IS_POLL)) {
                        try {
                            this.ddbTable.deleteItem(DDB_HASH_KEY_PRIORITY, priority, DDB_RANGE_KEY_UUID, uuid);
                            decrementSize();
                        } catch (Exception e) {
                            jedis.evalsha(this.addScriptSHA, 2, this.appName, Long.toString(priority));
                        }
                    }
                }
            }
        }
        return null;
    }

    public synchronized void addEndpoint(final String hostname, final String password) {
        this.addEndpoint(hostname, DEFAULT_PORT, password);
    }

    public synchronized void addEndpoint(final String hostname, final int port, final String password) {
        Jedis newJedis = new Jedis(hostname, port);
        newJedis.auth(password);
        this.jedis.add(newJedis);
    }

    public synchronized void addEndpoint(final String hostname) {
        this.addEndpoint(hostname, DEFAULT_PORT);
    }

    public synchronized void addEndpoint(final String hostname, final int port) {
        this.jedis.add(new Jedis(hostname, port));
    }

    public synchronized void removeAllEndpoints(final String hostname) {
        this.jedis.removeIf(a -> a.getClient().getHost().equals(hostname));
    }

    public synchronized void removeEndpoint(final String hostname) {
        this.removeEndpoint(hostname, DEFAULT_PORT);
    }

    public synchronized void removeEndpoint(final String hostname, int port) {
        this.jedis.removeIf(a -> a.getClient().getHost().equals(hostname) && a.getClient().getPort() == port);
    }

    private Jedis getRandomJedis() {
        return this.jedis.get(ThreadLocalRandom.current().nextInt(0, this.jedis.size()));
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray(Object[] objects) {
        throw new UnsupportedOperationException();
    }
}
