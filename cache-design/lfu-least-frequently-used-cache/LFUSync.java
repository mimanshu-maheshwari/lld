import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CountDownLatch;

public class LFUSync<K, V> {

  private class Node {
    private K    key;
    private V    value;
    private int  freq;
    private Node prev;
    private Node next;

    Node() {
      key   = null;
      value = null;
      freq  = 1;
      prev  = null;
      next  = null;
    }

    Node(K key, V value) {
      this();
      this.key   = key;
      this.value = value;
    }

  }

  private class DoublyLinkedList {
    private Node head;
    private Node tail;
    private int size;

    DoublyLinkedList() {
      this.head = new Node();
      this.tail  = new Node();
      this.head.next = this.tail;
      this.tail.prev = this.head;
      this.size = 0;
    }

    void pop(Node node) {
      if (this.size == 0){
        return;
      }
      Node prev = node.prev , 
           next = node.next;
      prev.next = next;
      next.prev = prev;
      node.next = null;
      node.prev = null;
      size--;
    }

    Node popHead() {
      if (size == 0){
        return null;
      }
      Node node = this.head.next;
      this.pop(node);
      return node;
    }

    void pushTail(Node node){
      Node prev = this.tail.prev;
      prev.next = node;
      node.next = this.tail;
      this.tail.prev = node;
      node.prev = prev;
      this.size++;
    }

    boolean isEmpty() {
      return this.size == 0;
    }

  }

  private int                            capacity;
  private Map<K, Node>                   data;
  private Map<Integer, DoublyLinkedList> freq;
  private int                            lfuCount;
  private ReentrantLock                  lock;

  public LFUSync(int capacity) {
    if (capacity <= 0){
      throw new IllegalArgumentException("The capacity can't be less than 1");
    }
    this.capacity = capacity;
    this.data     = new HashMap<>();
    this.freq     = new HashMap<>();
    this.lfuCount = 0;
    this.lock     = new ReentrantLock();
  }

  private void counter(Node node) {
    this.freq.get(node.freq).pop(node);
    if (node.freq == this.lfuCount && this.freq.get(node.freq).isEmpty()) {
      this.lfuCount++;
      this.freq.remove(node.freq);
    }
    node.freq++;
    this.freq.computeIfAbsent(node.freq, k -> new DoublyLinkedList()).pushTail(node);
  }

  public V get(K key) {
    if (key == null) {
      throw new IllegalArgumentException("Key can't be null");
    }
    lock.lock();
    try {
      Node node = this.data.get(key);
      if (node == null) {
        return null;
      }
      this.counter(node);
      return node.value;
    }finally {
      lock.unlock();
    }
  }

  private void evict(){
    if (this.capacity > this.data.size()) {
      return;
    }
    Node toRemove = this.freq.get(this.lfuCount).popHead();
    if (this.freq.get(toRemove.freq).isEmpty()) {
      this.freq.remove(toRemove.freq);
    }
    this.data.remove(toRemove.key);
  }

  public V put(K key, V value) {
    if (key == null) {
      throw new IllegalArgumentException("Key can't be null");
    }
    lock.lock();
    try {
      Node node = this.data.get(key);
      if (node != null) {
        V oldValue = node.value;
        node.value = value;
        this.counter(node);
        return oldValue;
      }
      this.evict();
      node = new Node(key, value);
      this.lfuCount = node.freq; // this is set to 1
      this.data.put(key, node);
      this.freq.computeIfAbsent(node.freq, k -> new DoublyLinkedList()).pushTail(node);
      return null;
    }finally {
      lock.unlock();
    }
  }

  private boolean validate() {
    lock.lock();
    try {
      int count = 0;
      for (var entry: freq.entrySet()) {
        var list = entry.getValue();
        var freq = entry.getKey();
        if (list.isEmpty()) {
          continue;
        }
        Node curr = list.head;
        while ((curr = curr.next) != list.tail) {
          if (curr.freq != freq) {
            throw new IllegalStateException("Frequency of node is not same as of list");
          }
          if (curr.next.prev != curr) {
            throw new IllegalStateException("Node next/prev invalid pointer");
          }
          if (curr.prev.next != curr) {
            throw new IllegalStateException("Node prev/next invalid pointer");
          }
          count++;
        }
      }
      if (count != this.data.size()) {
        throw new IllegalStateException("Node count is not equal to key count");
      }
      return true;
    }finally {
      lock.unlock();
    }
  }

  public static void main(String[] args) {
    try {
      test_usage_no_threads();
      test_usage_multi_thread();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void test_usage_multi_thread() {
    ExecutorService executors = Executors.newFixedThreadPool(5);
    CountDownLatch latch = new CountDownLatch(30);
    var lfu = new LFUSync<Integer, Integer>(3);
    for (int i = 0; i < 15; ++i) {
      final int key = i % 7;
      final int value = i;
      executors.execute(() -> {
        lfu.put(key, value);
        lfu.validate();
        latch.countDown();
      });
      executors.execute(() -> {
        lfu.get(key);
        lfu.validate();
        latch.countDown();
      });
    }

    try {
      latch.await();
      executors.shutdownNow();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private static void test_usage_no_threads(){ 
    var lfu = new LFUSync<Integer, Integer>(3);
    lfu.put(1, 1);
    lfu.validate();
    lfu.put(2, 2);
    lfu.validate();
    lfu.put(3, 3);
    lfu.validate();
    lfu.put(4, 4); // 1 should be evicted
    lfu.validate();
    {              
      var one = lfu.get(1);    // should be evicted
      lfu.validate();
      assert one == null: "One should be evicted";
    }
    {
      var two = lfu.get(2);    // should exist;
      lfu.validate();
      assert two != null: "Two should be present";
    }
    lfu.put(5, 5); // 3 should be evicted
    lfu.validate();
    {
      var three = lfu.get(3);    // should be evicted
      lfu.validate();
      assert three == null: "Three should be evicted";
    }
  }
}
