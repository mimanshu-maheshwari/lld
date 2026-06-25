import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

/**
 * Use lock striping/segmentation
 * Use a read-write lock
 * Use a concurrent cache design similar to Caffeine
 * Separate admission and eviction policies
 * SD3 level will be :
 *   HashMap<K, Node>
 *   DoublyLinkedList
 *   ReentrantLock
 */
public class LRUSyncTry2<K, V> {
  private final ConcurrentHashMap<K, V>  data;
  private final ConcurrentLinkedDeque<K> accessOrder;
  private final ReentrantLock            lock;
  private final int                      capacity;

  public LRUSyncTry2(int capacity) {
    this.capacity    = capacity;
    this.data        = new ConcurrentHashMap<>();
    this.accessOrder = new ConcurrentLinkedDeque<>();
    this.lock        = new ReentrantLock();
  }

  public V get(K key) {
    lock.lock();
    try {
      V value = data.get(key);
      if (value != null) {
        accessOrder.remove(key);
        accessOrder.addFirst(key);
      } 
      return value;
    } finally {
      lock.unlock();
    }
  }

  public V put(K key, V value) {
    lock.lock();
    try {
      V val = data.get(key);
      if (val != null) {
        data.put(key, value);
        accessOrder.remove(key);
        accessOrder.addFirst(key);
        return val;
      } else {
        if (capacity <= data.size()) {
          K keyToRemove = accessOrder.pollLast();
          data.remove(keyToRemove);
        }
        data.put(key, value);
        accessOrder.addFirst(key);
        return null;
      }
    }finally {
      lock.unlock();
    }
  }

  private void printState() {
    lock.lock();
    try {
      System.out.println(accessOrder);
      System.out.println(data);
    } finally {
      lock.unlock();
    }
  }

  public static void main(String[] args) {

    // validation test
    LRUSyncTry2<Integer, Integer> cache = new LRUSyncTry2<>(3);
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3);
    cache.get(1);      // make 1 most recent
    cache.put(4, 4);   // should evict 2
    assert cache.get(2) == null;
    assert cache.get(1) == 1;
    assert cache.get(3) == 3;
    assert cache.get(4) == 4;
    // ------------------
    int capacity = 3;
    var lruCache = new LRUSyncTry2<String, String>(capacity);
    var executor = Executors.newFixedThreadPool(5);
    var latch = new CountDownLatch(5);
    
    for (int i = 0; i < 5; ++i) {
      int threadId = i;
      executor.execute(() -> {
        {
          String key = "Key" + threadId;
          String value = "Value" + threadId;
          lruCache.put(key, value);
          System.out.println("Thread-%d put %s".formatted(threadId, key));
        }
        for (int j = 0; j < 5; ++j) {
          String key = "Key" + j;
          String value = lruCache.get(key);
          if (value == null){
            System.out.println("Thread-%d no value found for key %s".formatted(threadId, key));
          } else {
            System.out.println("Thread-%d got %s : %s".formatted(threadId, key, value));
          }
        }
        latch.countDown();
      });
    }
    try {
      latch.await();
      lruCache.printState();
      executor.shutdown();
      if (!executor.awaitTermination(2000, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ex) {
      ex.printStackTrace();
    }

  }
}
