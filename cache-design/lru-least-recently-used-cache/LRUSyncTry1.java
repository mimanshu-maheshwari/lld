import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ISSUES:
 * 1. No proper test cases for cycles, size, capacity
 * 2. No fine grained locking mechanism
 */
public class LRUSyncTry1<K, V> {

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

  public LRUSyncTry1(int capacity) {
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

  public static void main(String[] args) {
    var lruCache = new LRUSyncTry1<Integer, Integer>(4);
    int size = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(size);
    CountDownLatch latch = new CountDownLatch(200);
    Thread t1 = new Thread(() -> {
      for (int i = 0; i < 100; ++i) {
        final int key = i % 5;
        final int value = i;
        executorService.execute(() -> {
          Integer val = lruCache.put(key, value);
          if (val == null) {
            System.out.println(
                Thread.currentThread().getName() + " | Added a new entry for key " + key + " and value " + value);
          } else {
            System.out
                .println(
                    Thread.currentThread().getName() + " | Updated entry for key " + key + " and old value " + val
                        + " with new value " + value);
          }
          latch.countDown();
        });
      }
    }, "Add thread");
    Thread t2 = new Thread(() -> {
      for (int i = 0; i < 100; ++i) {
        final int key = i % 5;
        executorService.execute(() -> {
          Integer value = lruCache.get(key);
          if (value == null) {
            System.out.println(Thread.currentThread().getName() + " | Value for key : " + key + " not found.");
          } else {
            System.out.println(Thread.currentThread().getName() + " | Value for key : " + key + " is : " + value);
          }
          latch.countDown();
        });
      }
    }, "Get thread");

    try {
      t1.start();
      t2.start();

      t1.join();
      t2.join();
      latch.await(5000, TimeUnit.SECONDS);
      executorService.shutdown();
      executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      ex.printStackTrace();
    } finally {
      executorService.shutdownNow();
    }
  }
}
