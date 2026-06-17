import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Try1 {
  public static void main(String[] args) {
    try (var broker = new Broker()) {
      broker.createTopic("payment");
      broker.createTopic("refund");
      var subscriber1 = new Subscriber(broker, (message) -> {
        System.out.println("recived message for subscriber " + message);
      });
      subscriber1.subscribe("payment");
      var subscriber2 = new Subscriber(broker, (message) -> {
        System.out.println("recived message for subscriber " + message);
      });
      subscriber2.subscribe("refund");
      var publisher1 = new Publisher(broker);
      var publisher2 = new Publisher(broker);
      var publisher3 = new Publisher(broker);
      var publisher4 = new Publisher(broker);
      var t1 = new Thread(() -> {
        for (int i = 0; i < 12; ++i) {
          publisher1.publish("payment", "t1", i + "");
        }
      });
      var t2 = new Thread(() -> {
        for (int i = 0; i < 12; ++i) {
          publisher2.publish("refund", "t2", i + "");
        }
      });
      var t3 = new Thread(() -> {
        for (int i = 0; i < 12; ++i) {
          publisher3.publish("refund", "t3", i + "");
        }
      });
      var t4 = new Thread(() -> {
        for (int i = 0; i < 12; ++i) {
          publisher4.publish("payment", "t4", i + "");
        }
      });
      t1.start();
      t2.start();
      t3.start();
      t4.start();
      try {
        t1.join();
        t2.join();
        t3.join();
        t4.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

class Publisher {
  private Broker broker;
  private String id;

  public Publisher(Broker broker) {
    this.id = UUID.randomUUID().toString();
    this.broker = broker;
  }

  public String getId() {
    return id;
  }

  public void publish(String topic, String key, String value) {
    this.broker.publish(key, value, topic);
  }
}

interface MessageListener {
  public void listen(Message message);
}

class Subscriber {
  private String id;
  private Broker broker;
  private MessageListener listener;

  public Subscriber(Broker broker, MessageListener listener) {
    this.id = UUID.randomUUID().toString();
    this.broker = broker;
    this.listener = listener;
  }

  public String getId() {
    return id;
  }

  public void subscribe(String topic) {
    this.broker.subscribe(this, topic);
  }

  public void unsubscribe(String topic) {
    this.broker.unsubscribe(this, topic);
  }

  public MessageListener getListener() {
    return listener;
  }

}

class Broker implements AutoCloseable {
  private Map<String, Set<Subscriber>> consumers;
  private Map<String, Topic> topics;
  private ScheduledExecutorService scheduledExecutorService;
  private Map<String, ScheduledFuture<?>> pollers;
  // private ExecutorService deliveryPool;
  private String id;

  public Broker() {
    this.consumers = new ConcurrentHashMap<>();
    this.topics = new ConcurrentHashMap<>();
    this.scheduledExecutorService = Executors.newScheduledThreadPool(4);
    this.pollers = new ConcurrentHashMap<>();
    // this.deliveryPool = Executors.newCachedThreadPool();
    this.id = UUID.randomUUID().toString();
    System.out.println("Broker created [" + id + "]");
  }

  public boolean createTopic(String name) {
    if (this.topics.containsKey(name)) {
      return false;
    }
    var topic = new Topic(name);
    topics.put(name, topic);
    consumers.put(name, ConcurrentHashMap.newKeySet());
    System.out.println("Broker create topic for [" + id + "] for topic " + topic + "]");
    return true;
  }

  public boolean subscribe(Subscriber subscriber, String topic) {
    if (subscriber == null || topic == null || topic.trim().isEmpty() || !this.topics.containsKey(topic)) {
      return false;
    }
    this.consumers.get(topic).add(subscriber);
    System.out.println(
        "Broker subscribe for [" + id + "] for topic " + topic + "] subscriber [" + subscriber.getId() + "]");
    this.startPolling(subscriber, topic);
    return true;
  }

  public void unsubscribe(Subscriber subscriber, String topic) {
    if (Objects.isNull(subscriber) || Objects.isNull(topic) || topic.isEmpty() || !topics.containsKey(topic)) {
      return;
    }
    this.consumers.get(topic).remove(subscriber);
    System.out.println(
        "Broker unsubscribe for [" + id + "] for topic " + topic + "] subscriber [" + subscriber.getId() + "]");
    this.stopPolling(subscriber, topic);
  }

  public void startPolling(Subscriber subscriber, String topic) {
    ScheduledFuture<?> future = this.scheduledExecutorService.scheduleAtFixedRate(() -> {
      try {
        deliverMessage(topic, subscriber);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }, 0, 100, TimeUnit.MILLISECONDS);

    System.out.println(
        "Broker start polling for [" + id + "] for topic " + topic + "] subscriber [" + subscriber.getId() + "]");
    var prev = this.pollers.put(subscriber.getId(), future);
    if (prev != null) {
      prev.cancel(false);
    }
  }

  public void deliverMessage(String topicName, Subscriber subscriber) {
    var topic = this.topics.get(topicName);
    if (topic == null) {
      return;
    }
    var message = topic.consume();
    if (message == null) {
      return;
    }

    System.out.println(
        "Broker deliver message for [" + id + "] for topic " + topic + "] subscriber [" + subscriber.getId() + "]");
    subscriber.getListener().listen(message);
  }

  public void stopPolling(Subscriber subscriber, String topic) {
    var future = pollers.remove(subscriber.getId());
    System.out.println(
        "Broker stop polling for [" + id + "] for topic " + topic + "] subscriber [" + subscriber.getId() + "]");
    if (future != null) {
      future.cancel(false);
    }
  }

  public boolean publish(String key, String value, String topic) {
    if (Objects.isNull(key) || Objects.isNull(value) || Objects.isNull(topic) || !this.topics.containsKey(topic)) {
      return false;
    }
    var message = new Message(key, value, LocalDateTime.now());
    this.topics.get(topic).publish(message);
    System.out.println("Broker publish [" + id + "] for topic " + topic + "]");
    return true;
  }

  public Map<String, Set<Subscriber>> getConsumers() {
    return consumers;
  }

  public Map<String, Topic> getTopics() {
    return topics;
  }

  public String getId() {
    return id;
  }

  @Override
  public void close() throws Exception {
    this.scheduledExecutorService.shutdown();
    if (!this.scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
      this.scheduledExecutorService.shutdownNow();
    }
  }
}

class Topic {
  private List<Partition> partitions;
  private String name;

  public Topic(String name) {
    this.name = name;
    this.partitions = Collections.synchronizedList(new ArrayList<>());
    this.partitions.add(new Partition(name));
    System.out.println("Topic created [" + name + "]");
  }

  public String getName() {
    return this.name;
  }

  public boolean publish(Message message) {
    // can be based on multiple stratages like round robin or random or by key
    var partition = this.getPartition();
    partition.append(message);
    System.out.println("Topic publish [" + name + "]");
    return true;
  }

  public Partition getPartition() {
    var random = new Random();
    int index = random.nextInt(partitions.size());
    return this.partitions.get(index);
  }

  public Message consume() {
    for (var partition : partitions) {
      var message = partition.read();
      if (message != null) {
        System.out.println("Topic consume [" + name + "]");
        return message;
      }
    }
    return null;
  }

  public List<Partition> getPartitions() {
    return partitions;
  }
}

record Message(String key, String value, LocalDateTime timestamp) {
}

class Partition {
  private int offset;
  private final ReadWriteLock rwLock;
  private final List<Message> messages;
  private String topic;
  private String partitionId;

  public Partition(String topic) {
    this.topic = topic;
    this.offset = 0;
    this.rwLock = new ReentrantReadWriteLock();
    this.messages = new ArrayList<>();
    partitionId = UUID.randomUUID().toString();
    System.out.println("Partition created [" + partitionId + "] for topic [" + topic + "]");
  }

  public String getTopic() {
    return topic;
  }

  public int getOffset() {
    return offset;
  }

  public void clear() {
    // clear a few entries based on ttl
  }

  public void setOffset(int offset) {
    var writeLock = this.rwLock.writeLock();
    try {
      writeLock.lock();
      this.offset = offset;
      System.out.println("Partition update offset [" + partitionId + "] for topic [" + topic + "]");
    } catch (Exception ex) {
      throw ex;
    } finally {
      writeLock.unlock();
    }
  }

  public Message read() {
    var writeLock = this.rwLock.writeLock();
    try {
      writeLock.lock();
      if (offset >= this.messages.size()) {
        return null;
      }
      // update this logic to be reactive
      System.out.println("Partition read [" + partitionId + "] for topic [" + topic + "]");
      return this.messages.get(this.offset++);
    } catch (Exception exception) {
      throw exception;
    } finally {
      writeLock.unlock();
    }
  }

  public void append(Message message) {
    var writeLock = this.rwLock.writeLock();
    try {
      writeLock.lock();
      this.messages.add(message);
      System.out.println("Partition append [" + partitionId + "] for topic [" + topic + "]");
    } catch (Exception exception) {
      throw exception;
    } finally {
      writeLock.unlock();
    }
  }

  public String getPartitionId() {
    return partitionId;
  }
}
