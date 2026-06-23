import java.util.Map;
import java.util.HashMap;

/**
 * This is a Thread unsafe implementation for Least Recently Used cache
 */
public class LRU_Sync<K, V> {
  private Map<K, Node<K, V>> nodes;
  private Node<K, V> head, tail;
  private int size;
  private int capacity;

  private class Node<K,V> {
    K key;
    V value;
    Node<K, V> next, prev;

    public Node() {
      this.key   = null;
      this.value = null;
      this.next  = null;
      this.prev  = null;
    }

    public Node(K key, V value) {
      this.key   = key;
      this.value = value;
      this.next  = null;
      this.prev  = null;
    }

  }

  public LRU_Sync(int capacity) {
    this.capacity = capacity;
    this.nodes    = new HashMap<>();
    this.head     = new Node<>();
    this.tail     = new Node<>();
    head.next     = tail;
    tail.prev     = head;
    this.size     = 0;
  }

  private Node<K, V> pop(Node<K, V> node) {
    if (size == 0){
      return null;
    }
    Node<K, V> prev = node.prev, next = node.next;
    prev.next = next;
    next.prev = prev;
    node.next = null;
    node.prev = null;
    size--;
    return node;
  }

  private Node<K, V> popTail() {
    if (size == 0) {
      return null;
    }
    Node<K, V> node = tail.prev;
    pop(node);
    return node;
  }

  private void pushHead(Node<K, V> node) {
    Node<K, V> next = head.next;
    node.prev = head;
    node.next = next;
    next.prev = node;
    head.next = node;
    size++;
  }

  private void update(Node<K, V> node) {
    pop(node); 
    pushHead(node);
  }

  public V get(K key) {
    if (!nodes.containsKey(key)) {
      return null;
    }
    var node = nodes.get(key);
    update(node);
    return node.value;
  }

  private void evict() {
    var last = popTail();
    if (last == null){
      return;
    }
    nodes.remove(last.key);
  }

  public boolean put(K key, V value) {
    if (capacity == 0){
      return false;
    }

    if (nodes.containsKey(key)) {
      var node = nodes.get(key);
      node.value = value;
      update(node);
      return true;
    }

    if (capacity == nodes.size()) {
      evict();
    }

    Node<K, V> node = new Node<>(key, value);
    nodes.put(key, node);
    pushHead(node);
    return true;
  }

  public static void main(String[] args) {
    var lruCache = new LRU_Sync<Integer, Integer>(4);
    lruCache.put(1, 2);
    lruCache.put(2, 3);
    lruCache.put(3, 4);
    lruCache.put(5, 6);
    lruCache.put(1, 4);
    lruCache.put(4, 5);
    Integer val = lruCache.get(2);
    System.out.println(val == null ? "No value found" : val);
  }
}
