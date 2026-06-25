import java.util.Map;
import java.util.HashMap;

public class LFU<K, V> {

  private class Node {
    private K    key;
    private V    val;
    private int  freq;
    private Node next, prev;

    public Node(){
      this.freq = 1;
      this.next = null;
      this.prev = null;
    }

    public Node(K key, V value) {
      this();
      this.key = key;
      this.val = value;
    }

  }

  private class DoublyLinkedList {
    private Node left, right;
    private int  size;

    public DoublyLinkedList() {
      left       = new Node();
      right      = new Node();
      left.next  = right;
      right.prev = left;
      size = 0;
    }

    private void pushRight(Node node) {
      Node prev  = right.prev;
      prev.next  = node;
      node.next  = right;
      right.prev = node;
      node.prev  = prev;
      ++size;
    }

    private void pop(Node node) {
      if (size == 0){
        return;
      }
      Node prev = node.prev, next = node.next;
      prev.next = next;
      next.prev = prev;
      node.next = null;
      node.prev = null;
      --size;
    }

    private Node popLeft() {
      if (size == 0){
        return null;
      }
      Node node = left.next;
      pop(node);
      return node;
    }

    private boolean isEmpty() {
      return size == 0;
    } 
  }

  // key -> node
  private Map<K, Node>                   data;
  // freq -> doubly linked list
  // right most recent
  // left is oldest
  private Map<Integer, DoublyLinkedList> freq;
  private int                            lfuCount;
  private final int                      capacity;

  public LFU(int capacity) {
    if (capacity <= 0){
      throw new IllegalArgumentException("Capacity should be greater than 0");
    }
    this.capacity = capacity;
    this.lfuCount = 0;
    this.data     = new HashMap<>();
    this.freq     = new HashMap<>();
  }

  private void refresh(Node node) {
    freq.get(node.freq).pop(node);
    if (node.freq == lfuCount && freq.get(node.freq).isEmpty()) {
      freq.remove(node.freq);
      lfuCount++;
    }
    node.freq++;
    freq.computeIfAbsent(node.freq, k -> new DoublyLinkedList()).pushRight(node);
  }

  public void validate() {
    int count = 0;
    for (DoublyLinkedList dll : freq.values()) {
      Node curr = dll.left.next;
      while (curr != dll.right) {
        count++;
        if (curr.next.prev != curr) {
          throw new IllegalStateException("Broken next/prev");
        }
        if (curr.prev.next != curr) {
          throw new IllegalStateException("Broken prev/next");
        }
        curr = curr.next;
      }
    }
    if (count != data.size()) {
      throw new IllegalStateException(
          "Map size and list size mismatch");
    }
  }

  private void evict() {
    if (data.size() == capacity) {
      Node node = freq.get(lfuCount).popLeft();
      if (freq.get(lfuCount).isEmpty()) {
        freq.remove(lfuCount);
      }
      data.remove(node.key);
      System.out.println("Removed key %s with value %s".formatted(node.key, node.val));
    }
  }

  public V get(K key) {
    Node node = data.get(key);
    if (node == null){
      return null;
    }
    refresh(node);
    return node.val;
  }

  public V put(K key, V value) {
    Node node = data.get(key);
    if (node != null) {
      V prevValue = node.val;
      node.val = value;
      refresh(node);
      return prevValue;
    }
    evict();
    node = new Node(key, value);
    lfuCount = 1;
    data.put(key, node);
    freq.computeIfAbsent(lfuCount, k -> new DoublyLinkedList()).pushRight(node);
    return null;
  }

  public static void main(String[] args) {
    int capacity = 4;
    var lfu = new LFU<Integer, Integer>(capacity);
    for (int i = 0; i < 55; ++i) {
      Integer oldVal = lfu.put(i % 6, i);
      lfu.validate();
      if (oldVal == null){
        System.out.println("Inserted key %s with value %s".formatted((i % 6), i));
      } else {
        System.out.println("Updated key %s with value %s replacing %s".formatted((i % 6), i, oldVal));
      }
      Integer val = lfu.get(i % 5);
      if (val == null){
        System.out.println("%s Key not found".formatted((i % 5)));
      } else {
        System.out.println("%s Key found with value %s".formatted((i % 5), val));
      }
    }
  }
}


