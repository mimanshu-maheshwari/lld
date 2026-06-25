import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

interface Cache<K, V> {
  public V get(K key);
  public V put(K key, V value);
}

public class SegmentedLRUSync<K, V> implements Cache<K, V> {

  private class Segment {

    private class Node {
      private K key;
      private V val;
      private Node next, prev;

      Node() {
        this.key = null;
        this.val = null;
        this.next = null;
        this.prev = null;
      }

      Node(K key, V value) {
        this.key = key;
        this.val = value;
        this.next = null;
        this.prev = null;
      }

    }

    private final int capacity;
    private Map<K, Node> data;
    private Node left, right;
    private ReentrantLock lock;

    public Segment(int capacity) {
      if (capacity == 0) {
        throw new IllegalArgumentException("Capacity can't be zero!!");
      }
      this.lock = new ReentrantLock();
      this.capacity = capacity;
      this.data = new HashMap<>();
      this.left = new Node();
      this.right = new Node();
      this.left.next = this.right;
      this.right.prev = this.left;
    }

    private void pop(Node node) {
      if (node == left || node == right) {
        return;
      }
      Node prev = node.prev, next = node.next;
      prev.next = next;
      next.prev = prev;
      node.next = null;
      node.prev = null;
    }

    private Node popLeft() {
      if (left.next == right) {
        return null;
      }
      Node node = this.left.next;
      pop(node);
      return node;
    }

    private void pushRight(Node node) {
      Node prev = right.prev;
      prev.next = node;
      node.next = right;
      right.prev = node;
      node.prev = prev;
    }

    private void refresh(Node node) {
      pop(node);
      pushRight(node);
    }

    private void evict() {
      if (capacity > data.size()) {
        return;
      }
      Node node = popLeft();
      data.remove(node.key);
    }

    public V get(K key) {
      lock.lock();
      try {
        Node node = data.get(key);
        if (node == null) {
          return null;
        }
        refresh(node);
        return node.val;
      } finally {
        lock.unlock();
      }
    }

    public V put(K key, V value) {
      lock.lock();
      try {
        Node node = data.get(key);
        if (node != null) {
          V prev = node.val;
          node.val = value;
          refresh(node);
          return prev;
        }
        evict();
        node = new Node(key, value);
        data.put(key, node);
        pushRight(node);
        return null;
      } finally {
        lock.unlock();
      }
    }

    public int size() {
      return data.size();
    }
  }

  private final int totalCapacity;
  private final int segmentCount;
  private List<Segment> segments;

  public SegmentedLRUSync(int totalCapacity, int segmentCount) {
    if (totalCapacity <= 0 || segmentCount <= 0) {
      throw new IllegalArgumentException("Capacity and segmentCount must be positive");
    }
    this.totalCapacity = totalCapacity;
    this.segmentCount = segmentCount;
    this.segments = new ArrayList<>();
    int perSegmentCapacity = (totalCapacity + segmentCount - 1) / segmentCount;
    for (int i = 0; i < segmentCount; ++i) {
      segments.add(new Segment(perSegmentCapacity));
    }
  }

  private int hash(K key) {
    return (key.hashCode() & 0x7FFF_FFFF) % segmentCount;
  }

  public V get(K key) {
    int index = hash(key);
    return segments.get(index).get(key);
  }

  public V put(K key, V value){
    int index = hash(key);
    return segments.get(index).put(key, value);
  }

  public SegmentedLRUSync(int totalCapacity) {
    this(totalCapacity, 4);
  }


  public static void main(String[] args) throws Exception {
    // Basic put/get
    SegmentedLRUSync<Integer, String> cache =
      new SegmentedLRUSync<>(4, 2);

    assert cache.get(1) == null;
    assert cache.put(1, "one") == null;
    assert cache.put(2, "two") == null;
    assert "one".equals(cache.get(1));
    assert "two".equals(cache.get(2));

    // Update existing key
    assert "one".equals(cache.put(1, "ONE"));
    assert "ONE".equals(cache.get(1));

    // LRU eviction test
    // segmentCount=2
    // keys 2,4,6 all hash to segment 0
    cache.put(4, "four");

    // access 4 so 2 becomes LRU
    assert "four".equals(cache.get(4));
    cache.put(6, "six");
    assert cache.get(2) == null;
    assert "four".equals(cache.get(4));
    assert "six".equals(cache.get(6));

    // Concurrent sanity test
    SegmentedLRUSync<Integer, Integer> concurrentCache =
      new SegmentedLRUSync<>(1000, 8);
    int threads = 8;
    int ops = 10_000;
    ExecutorService executor =
      Executors.newFixedThreadPool(threads);
    CountDownLatch latch =
      new CountDownLatch(threads);
    for (int t = 0; t < threads; t++) {
      final int tid = t;
      executor.submit(() -> {
        try {
          for (int i = 0; i < ops; i++) {
            int key = (tid * ops + i) % 2000;
            concurrentCache.put(key, i);
            Integer val = concurrentCache.get(key);
            assert val != null;
          }
        } finally {
          latch.countDown();
        }
      });
    }
    latch.await();
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);

    for (var segment : concurrentCache.segments) {
      assert segment.size() <= segment.capacity;
    }

    System.out.println("All tests passed.");
  }
}
