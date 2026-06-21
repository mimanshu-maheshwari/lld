import java.awt.Insets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Try1 {
  public static void main(String[] args) {
    var rateLimiterService = new RateLimiterService();
    var user1 = new User("user1", UserTier.FREE);
    var user2 = new User("user2", UserTier.PREMIUM);
    var executerService = Executors.newFixedThreadPool(4);
    var t1 = new Thread(() -> {
      for (int i = 0; i < 30; ++i) {
        final int rid = i;
        executerService.execute(() -> {
          var allowed = rateLimiterService.allowRequest(user1);
          System.out.println(
              "For request : " + rid + " user: " + user1 + " is" + (allowed ? "" : " not") + " allowed to access.");
        });
        if (i == 15) {
          System.out.println("Waiting for " + user1 + "for a second");
          try {
            Thread.sleep(Duration.ofSeconds(1));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 1; i <= 40; ++i) {
        final int rid = i;
        executerService.execute(() -> {
          var allowed = rateLimiterService.allowRequest(user2);
          System.out.println(
              "For request : " + rid + " user: " + user2 + " is" + (allowed ? "" : " not") + " allowed to access.");
        });
        if (i == 25) {
          System.out.println("Waiting for " + user2 + "for a second");
          try {
            Thread.sleep(Duration.ofSeconds(1));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });

    t1.start();
    t2.start();
    try {
      t1.join();
      t2.join();
      executerService.shutdown();
      if (!executerService.awaitTermination(2_000, TimeUnit.SECONDS)) {
        executerService.shutdownNow();
      }
      rateLimiterService.shutdown();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

// create a rate limiter.
// This is a service side rate limiter
// Rate limi user based on UserId and tier (free or premium)
// Support for types (and extensible)
// thread safe and efficient

enum UserTier {
  FREE, PREMIUM,
}

record User(String id, UserTier tier) {
}

record RateLimiterConfig(int maxRequests, int timeWindowInSeconds, long timeToLiveSeconds) {
}

abstract sealed class RateLimiter permits TokenBucket, FixedWindow, SlidingWindowCounter, SlidingWindowLogs {
  final RateLimiterConfig config;
  final RateLimiterType type;
  private final ScheduledExecutorService scheduler;

  public RateLimiter(RateLimiterConfig config, RateLimiterType type) {
    this.config = config;
    this.type = type;
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    this.scheduler.scheduleAtFixedRate(this::cleanup, 10, config.timeWindowInSeconds(), TimeUnit.SECONDS);
  }

  public abstract boolean allowRequest(User user);

  public abstract void cleanup();

  public void shutdown() throws InterruptedException {
    this.scheduler.shutdown();
    while (!this.scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
      this.scheduler.shutdownNow();
    }
  }

  public RateLimiterConfig getConfig() {
    return config;
  }

  public RateLimiterType getType() {
    return type;
  }

}

final class TokenBucket extends RateLimiter {
  private class Bucket {
    private int tokens;
    private long lastRefillTime;
    private ReadWriteLock rwLock;
    private long lastAccessTime;

    private Bucket() {
      this.rwLock = new ReentrantReadWriteLock();
    }

    public Bucket(int tokens, long lastRefillTime) {
      this();
      this.tokens = tokens;
      this.lastRefillTime = lastRefillTime;
      this.lastAccessTime = Instant.now().getEpochSecond();
    }

    public long getLastAccessTime() {
      return lastAccessTime;
    }

    public boolean allowReqeust(RateLimiterConfig config, long now) {
      var writeLock = rwLock.writeLock();
      writeLock.lock();
      this.lastAccessTime = Instant.now().getEpochSecond();
      try {
        refillTokens(config, now);
        if (tokens > 0) {
          --tokens;
          return true;
        }
        return false;
      } finally {
        writeLock.unlock();
      }
    }

    private void refillTokens(RateLimiterConfig config, long now) {
      double refillRate = (double) config.maxRequests() / config.timeWindowInSeconds();
      long elapsedSeconds = now - lastRefillTime;

      double refillTokens = (elapsedSeconds * refillRate);
      tokens = (int) Math.ceil(Math.min(tokens + refillTokens, config.maxRequests()));
      if (tokens > 0) {
        lastRefillTime = now;
      }
    }
  }

  private final Map<String, Bucket> buckets;

  public TokenBucket(RateLimiterConfig config, RateLimiterType type) {
    super(config, type);
    this.buckets = new ConcurrentHashMap<>();
  }

  @Override
  public boolean allowRequest(User user) {
    final long now = System.currentTimeMillis() / 1_000;
    var bucket = buckets.computeIfAbsent(user.id(), (id) -> {
      var b = new Bucket(config.maxRequests(), now);
      return b;
    });
    return bucket.allowReqeust(config, now);
  }

  @Override
  public void cleanup() {
    this.buckets.entrySet().removeIf(
        entry -> (Instant.now().getEpochSecond() - entry.getValue().lastAccessTime) >= config.timeToLiveSeconds());
  }

}

final class FixedWindow extends RateLimiter {

  private class RequestWindowCount {
    private int requestCount;
    private long windowStart;
    private ReadWriteLock rwLock;
    private long lastAccessTime;

    private RequestWindowCount() {
      this.rwLock = new ReentrantReadWriteLock();
      this.requestCount = 0;
      this.lastAccessTime = Instant.now().getEpochSecond();
    }

    public RequestWindowCount(long now) {
      this();
      this.windowStart = now;
    }

    public boolean isAllowed(int maxRequests, long currentWindow) {
      var writeLock = rwLock.writeLock();
      writeLock.lock();
      try {
        this.lastAccessTime = Instant.now().getEpochSecond();
        if (windowStart != currentWindow) {
          windowStart = currentWindow;
          requestCount = 1;
          return true;
        }

        if (requestCount < maxRequests) {
          ++requestCount;
          return true;
        }
        return false;
      } finally {
        writeLock.unlock();
      }
    }
  }

  private final Map<String, RequestWindowCount> requestWindowCount;

  public FixedWindow(RateLimiterConfig config, RateLimiterType type) {
    super(config, type);
    this.requestWindowCount = new ConcurrentHashMap<>();
  }

  @Override
  public boolean allowRequest(User user) {
    long currentWindow = System.currentTimeMillis() / 1_000 / config.timeWindowInSeconds();
    var requestWindow = requestWindowCount.computeIfAbsent(user.id(), (id) -> {
      return new RequestWindowCount(currentWindow);
    });
    return requestWindow.isAllowed(config.maxRequests(), currentWindow);
  }

  @Override
  public void cleanup() {
    this.requestWindowCount.entrySet().removeIf(
        entry -> (Instant.now().getEpochSecond() - entry.getValue().lastAccessTime) >= config.timeToLiveSeconds());
  }
}

final class SlidingWindowLogs extends RateLimiter {
  private final Map<String, Log> requestLog;

  private class Log {
    private ConcurrentLinkedDeque<Long> log;
    private long lastAccessTime;

    public Log() {
      this.log = new ConcurrentLinkedDeque<>();
      this.lastAccessTime = Instant.now().getEpochSecond();
    }

    public boolean isAllowed(RateLimiterConfig config, long now) {
      this.lastAccessTime = Instant.now().getEpochSecond();
      while (!log.isEmpty() && (now - log.peek()) >= config.timeWindowInSeconds()) {
        log.poll();
      }
      if (log.size() < config.maxRequests()) {
        log.offer(now);
        return true;
      }
      return false;
    }
  }

  public SlidingWindowLogs(RateLimiterConfig config, RateLimiterType type) {
    super(config, type);
    this.requestLog = new ConcurrentHashMap<>();
  }

  @Override
  public boolean allowRequest(User user) {
    long now = System.currentTimeMillis() / 1_000;
    var log = requestLog.compute(user.id(), (id, logs) -> new Log());
    return log.isAllowed(config, now);
  }

  @Override
  public void cleanup() {
    this.requestLog.entrySet().removeIf(
        entry -> (Instant.now().getEpochSecond() - entry.getValue().lastAccessTime) >= config.timeToLiveSeconds());
  }
}

final class SlidingWindowCounter extends RateLimiter {

  private class Slider {
    private long currentWindowStart, previousWindowCount, currentWindowCount;
    private ReadWriteLock rwLock;
    private long lastAccessTime;

    public Slider() {
      this.rwLock = new ReentrantReadWriteLock();
      this.currentWindowStart = Instant.now().getEpochSecond();
      this.currentWindowCount = 0;
      this.previousWindowCount = 0;
      this.lastAccessTime = Instant.now().getEpochSecond();
    }

    public boolean computeIsAllowed(RateLimiterConfig config, long now) {
      var writeLock = rwLock.writeLock();
      try {
        writeLock.lock();
        this.lastAccessTime = Instant.now().getEpochSecond();
        long elapsedSeconds = now - currentWindowStart;
        if (elapsedSeconds >= config.timeWindowInSeconds()) {
          previousWindowCount = currentWindowCount;
          currentWindowCount = 0;
          currentWindowStart = now;
        }
        double weightedCount = previousWindowCount
            * ((config.timeWindowInSeconds() - elapsedSeconds) / (double) config.timeWindowInSeconds())
            + currentWindowCount;
        if (weightedCount < config.maxRequests()) {
          ++currentWindowCount;
          return true;
        }
        return false;
      } finally {
        writeLock.unlock();
      }
    }
  }

  private final Map<String, Slider> sliders;

  public SlidingWindowCounter(RateLimiterConfig config, RateLimiterType type) {
    super(config, type);
    this.sliders = new ConcurrentHashMap<>();
  }

  @Override
  public boolean allowRequest(User user) {
    long now = Instant.now().getEpochSecond();
    var slider = sliders.computeIfAbsent(user.id(), id -> {
      return new Slider();
    });
    return slider.computeIsAllowed(config, now);
  }

  @Override
  public void cleanup() {
    this.sliders.entrySet().removeIf(
        entry -> (Instant.now().getEpochSecond() - entry.getValue().lastAccessTime) >= config.timeToLiveSeconds());
  }
}

enum RateLimiterType {

  TOKEN_BUCKET {

    @Override
    public RateLimiter create(RateLimiterConfig config) {
      return new TokenBucket(config, this);
    }
  },
  FIXED_WINDOW {

    @Override
    public RateLimiter create(RateLimiterConfig config) {
      return new FixedWindow(config, this);
    }

  },
  SLIDING_WINDOW_COUNTER {

    @Override
    public RateLimiter create(RateLimiterConfig config) {
      return new SlidingWindowCounter(config,
          this);
    }

  },
  SLIDING_WINDOW_LOGS {

    @Override
    public RateLimiter create(RateLimiterConfig config) {
      return new SlidingWindowLogs(config, this);
    }
  };

  public abstract RateLimiter create(RateLimiterConfig config);
}

final class RateLimiterFactory {
  public static RateLimiter createRateLimiter(RateLimiterType type, RateLimiterConfig config) {
    return type.create(config);
    // // other way is this
    // return switch (type) {
    // case RateLimiterType.FIXED_WINDOW -> new FixedWindow(config, type);
    // case RateLimiterType.TOKEN_BUCKET -> new TokenBucket(config, type);
    // case RateLimiterType.SLIDING_WINDOW_COUNTER -> new
    // SlidingWindowCounter(config, type);
    // case RateLimiterType.SLIDING_WINDOW_LOGS -> new SlidingWindowLogs(config,
    // type);
    // };
  }
}

class RateLimiterService {
  private final Map<UserTier, RateLimiter> rateLimiters;

  public RateLimiterService() {
    this.rateLimiters = new HashMap<>();

    // 10 request/minute for free user with fixed window algorithm.
    rateLimiters.put(
        UserTier.FREE,
        RateLimiterFactory.createRateLimiter(
            RateLimiterType.FIXED_WINDOW,
            new RateLimiterConfig(10, 60, 1_800)));

    // 20 request/minute for premium user with Token bucket algorithm.
    rateLimiters.put(
        UserTier.PREMIUM,
        RateLimiterFactory.createRateLimiter(
            RateLimiterType.TOKEN_BUCKET,
            new RateLimiterConfig(20, 60, 3_600)));
  }

  public boolean allowRequest(User user) {
    var rateLimiter = this.rateLimiters.get(user.tier());
    if (rateLimiter == null) {
      throw new IllegalArgumentException("Invalid user tier.");
    }
    return rateLimiter.allowRequest(user);
  }

  public void shutdown() {
    this.rateLimiters.values().forEach(rl -> {
      try {
        rl.shutdown();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
  }
}
