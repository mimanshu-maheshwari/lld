import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Try2 {
  private static final String PAYMENT = "PAYMENT";
  private static final String REFUND = "REFUND";

  public static void main(String[] args) {
    Broker broker = new Broker();
    broker.createTopic(PAYMENT, 2);
    broker.createTopic(REFUND, 3);
    Subscriber sub1 = new Subscriber() {
      public String getId() {
        return "[Subscriber:1]";
      }

      public void run() {
        this.getBroker().subscribe(this, PAYMENT);
        try {
          Thread.sleep(Duration.ofMillis(4000));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        this.getBroker().unsubscribe(this, PAYMENT);
      }

      public MessageListener getListener() {
        return (message) -> System.out.println("Message: [" + message + "] recieved to " + getId());
      }

      public Broker getBroker() {
        return broker;
      }
    };

    Subscriber sub2 = new Subscriber() {
      public String getId() {
        return "[Subscriber:2]";
      }

      public void run() {
        this.getBroker().subscribe(this, PAYMENT);
        try {
          Thread.sleep(Duration.ofMillis(6000));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        this.getBroker().unsubscribe(this, PAYMENT);
      }

      public MessageListener getListener() {
        return (message) -> System.out.println("Message: [" + message + "] recieved to " + getId());
      }

      public Broker getBroker() {
        return broker;
      }
    };

    Subscriber sub3 = new Subscriber() {
      public String getId() {
        return "[Subscriber:3]";
      }

      public void run() {
        this.getBroker().subscribe(this, REFUND);
        try {
          Thread.sleep(Duration.ofMillis(5000));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        this.getBroker().unsubscribe(this, REFUND);
      }

      public MessageListener getListener() {
        return (message) -> System.out.println("Message: [" + message + "] recieved to " + getId());
      }

      public Broker getBroker() {
        return broker;
      }
    };

    Publisher pub1 = new Publisher() {
      public String getId() {
        return "[Publisher:1]";
      }

      public Broker getBroker() {
        return broker;
      }

      public void run() {
        for (int i = 1; i < 6; ++i) {
          this.getBroker().publish("payment", "Payment  " + i + " initiated by " + getId(), PAYMENT);
        }
      }

    };
    Publisher pub2 = new Publisher() {
      public String getId() {
        return "[Publisher:2]";
      }

      public Broker getBroker() {
        return broker;
      }

      public void run() {
        for (int i = 1; i < 6; ++i) {
          this.getBroker().publish("payment", "Payment  " + i + " initiated by " + getId(), PAYMENT);
        }
      }

    };
    Publisher pub3 = new Publisher() {
      public String getId() {
        return "[Publisher:3]";
      }

      public Broker getBroker() {
        return broker;
      }

      public void run() {
        for (int i = 1; i < 6; ++i) {
          this.getBroker().publish("payment", "Payment  " + i + " initiated by " + getId(), PAYMENT);
        }
      }

    };
    Thread ts1 = new Thread(sub1);
    Thread ts2 = new Thread(sub2);
    Thread ts3 = new Thread(sub3);

    Thread tp1 = new Thread(pub1);
    Thread tp2 = new Thread(pub2);
    Thread tp3 = new Thread(pub3);

    ts1.start();
    ts2.start();
    ts3.start();

    tp1.start();
    tp2.start();
    tp3.start();

    try {
      ts1.join();
      ts2.join();
      ts3.join();

      tp1.join();
      tp2.join();
      tp3.join();
      Thread.sleep(Duration.ofMillis(10000));
      broker.shutdown();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

interface Publisher extends Runnable {
  public String getId();

  public Broker getBroker();

  default void publish(String key, String value, String topic) {
    this.getBroker().publish(key, value, topic);
  }
}

interface Subscriber extends Runnable {
  public String getId();

  public MessageListener getListener();

  public Broker getBroker();

  default void subscribe(String topic) {
    this.getBroker().subscribe(this, topic);
  }

  default void unsubscribe(String topic) {
    this.getBroker().unsubscribe(this, topic);
  }
}

record Message(String key, String value, LocalDateTime timestamp, Map<String, Object> metadata) {
  public Message(String key, String value) {
    this(key, value, LocalDateTime.now(), new HashMap<>());
  }
}

@FunctionalInterface
interface MessageListener {
  public void listen(Message message);
}

class Topic {
  private static int counter = 0;

  private record LogPointer(int partitionIndex, int offset) {
  }

  private final String name;
  private final String topicId;
  private final List<Partition> partitions;
  private final List<LogPointer> log;
  // subscriber id -> next log index
  private final Map<String, Integer> consumerOffsets;
  private int roundRobin;
  private ReadWriteLock rwLock;

  // think of way where we can track multiple partitions
  // such that the offset is kept in track and
  // next partition and offset for that partition should be consumed

  public Topic(String name) {
    this.name = name;
    this.topicId = "" + (counter++);
    this.partitions = new ArrayList<>();
    this.partitions.add(new Partition());
    this.log = new ArrayList<>();
    this.consumerOffsets = new ConcurrentHashMap<>();
    this.roundRobin = 0;
    this.rwLock = new ReentrantReadWriteLock();
  }

  public Topic(String name, int partitionCount) {
    this(name);
    for (int i = 1; i < partitionCount; ++i) {
      this.partitions.add(new Partition());
    }
  }

  public String getName() {
    return name;
  }

  public String getTopicId() {
    return topicId;
  }

  public void registerConsumer(String subscriberId) {
    var writeLock = rwLock.writeLock();
    writeLock.lock();
    try {
      consumerOffsets.putIfAbsent(subscriberId, 0);
    } finally {
      writeLock.unlock();
    }
  }

  public void removeConsumer(String subscriberId) {
    var writeLock = rwLock.writeLock();
    writeLock.lock();
    try {
      consumerOffsets.remove(subscriberId);
    } finally {
      writeLock.unlock();
    }
  }

  public Optional<Message> readMessage(String subscriberId) {
    var writeLock = rwLock.writeLock();
    writeLock.lock();
    try {
      Integer idx = consumerOffsets.get(subscriberId);
      if (idx == null || idx >= log.size()) {
        return Optional.empty();
      }
      consumerOffsets.put(subscriberId, idx + 1);
      var logPointer = log.get(idx);
      return partitions.get(logPointer.partitionIndex()).read(logPointer.offset());
    } finally {
      writeLock.unlock();
    }
  }

  public void publishMessage(String key, String value) {
    var message = new Message(key, value);
    pushToPartition(message);
  }

  private void pushToPartition(Message message) {
    var writeLock = rwLock.writeLock();
    writeLock.lock();
    try {
      int p = roundRobin++ % partitions.size();
      int offset = this.partitions.get(p).append(message);
      this.log.add(new LogPointer(p, offset));
    } finally {
      writeLock.unlock();
    }
  }
}

class Partition {
  private static int counter = 0;
  private String partitionId;
  private ReadWriteLock rwLock;
  // can we use blocking queue here ?
  private List<Message> messages;

  public Partition() {
    this.partitionId = "" + (counter++);
    this.rwLock = new ReentrantReadWriteLock();
    this.messages = new ArrayList<>();
  }

  public String getPartitionId() {
    return partitionId;
  }

  public int append(Message message) {
    var writeLock = rwLock.writeLock();
    writeLock.lock();
    try {
      messages.add(message);
      return messages.size() - 1;
    } finally {
      writeLock.unlock();
    }
  }

  public Optional<Message> read(int offset) {
    var writeLock = rwLock.writeLock();
    writeLock.lock();
    try {
      if (offset >= messages.size()) {
        return Optional.empty();
      }
      var message = messages.get(offset);
      return Optional.of(message);
    } finally {
      writeLock.unlock();
    }
  }
}

class Broker {
  private static int counter = 0;
  private String brokerId;
  private Map<String, Set<Subscriber>> subscribers;
  private Map<String, Topic> topics;
  private Map<String, ScheduledFuture<?>> pollers;
  private ScheduledExecutorService pollerPool;

  public Broker() {
    this.brokerId = "" + (counter++);
    this.subscribers = new ConcurrentHashMap<>();
    this.topics = new ConcurrentHashMap<>();
    this.pollers = new ConcurrentHashMap<>();
    this.pollerPool = Executors.newScheduledThreadPool(4);
  }

  public String getBrokerId() {
    return brokerId;
  }

  public void createTopic(String topicName) {
    this.createTopic(topicName, 1);
  }

  public void createTopic(String topicName, int size) {
    topics.computeIfAbsent(topicName, k -> new Topic(topicName, size));
    subscribers.computeIfAbsent(topicName, k -> ConcurrentHashMap.newKeySet());
  }

  public void subscribe(Subscriber subscriber, String topicName) {
    Topic topic = topics.get(topicName);
    if (topic == null) {
      throw new IllegalArgumentException("Topic with name: [" + topicName + "] not found.");
    }
    this.subscribers.get(topicName).add(subscriber);
    topic.registerConsumer(subscriber.getId());
    this.startPolling(subscriber, topic);
  }

  public void unsubscribe(Subscriber subscriber, String topicName) {
    var topic = this.topics.get(topicName);
    var subs = this.subscribers.get(topicName);
    if (topic == null || subs == null) {
      return;
    }
    subs.remove(subscriber);
    this.stopPolling(subscriber, topicName);
    topic.removeConsumer(subscriber.getId());
  }

  public void publish(String key, String value, String topicName) {
    var topic = topics.get(topicName);
    if (topic == null) {
      throw new IllegalArgumentException("Topic with name: [" + topicName + "] not found.");
    }
    topic.publishMessage(key, value);
  }

  private String pollerKey(Subscriber subscriber, String topicName) {
    return subscriber.getId() + "::" + topicName;
  }

  private void startPolling(Subscriber subscriber, Topic topic) {
    String key = pollerKey(subscriber, topic.getName());
    ScheduledFuture<?> future = pollerPool.scheduleAtFixedRate(() -> {
      try {
        deliverMessage(subscriber, topic);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, 0, 100, TimeUnit.MILLISECONDS);
    var prev = pollers.put(key, future);
    if (prev != null) {
      prev.cancel(false);
    }
  }

  private void deliverMessage(Subscriber subscriber, Topic topic) {
    Optional<Message> next;
    while ((next = topic.readMessage(subscriber.getId())).isPresent()) {
      subscriber.getListener().listen(next.get());
    }
  }

  private void stopPolling(Subscriber subscriber, String topicName) {
    var future = pollers.remove(pollerKey(subscriber, topicName));
    if (future != null) {
      future.cancel(false);
    }
  }

  public void shutdown() throws InterruptedException {
    this.pollerPool.shutdown();
    if (!this.pollerPool.awaitTermination(10, TimeUnit.SECONDS)) {
      this.pollerPool.shutdownNow();
    }
  }
}
