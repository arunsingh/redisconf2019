class CacheAdapter
  DEFAULT_EXPIRE_IN = 86400

  # This method by and large works identically to #fetch_multi, except it allows you to cause a cache miss based
  # on some interval. You can use this in places where there will be a lot of contention on cache miss: as an example,
  # you can cache something for 90 seconds, but every 30 seconds have one process refresh the cache to extend it
  # by another 90 seconds. This way, we're not dealing with a thundering herd problem on cache misses, especially
  # if computing the data in the cache is hard to do.
  #
  # @param [String] refresh_key Key to be used to determine if we should refresh the cache
  # @param [Integer] refresh_interval Number of seconds to use to refresh the cache
  # @param [Array<String>] keys Array of String cache keys
  # @param [Integer] cache_expiry Optional integer of how long to expire the items in the Cache
  def self.fetch_multi_with_refresh(refresh_key, refresh_interval, keys, cache_expiry, &block)
    refresh_cache = false
    begin
      refresh_cache = self.redis.with {|r| r.set(refresh_key, true, :nx => true, :ex => refresh_interval)}
    rescue => e
      LOGGER.info {"#{self} could not check to refresh #{refresh_key} due to #{e.inspect()}"}
    end

    if refresh_cache
      # In this scenario, we grabbed the lock, so we want to refresh all the keys such that we keep
      # the cache updated
      needed_keys = keys

      # Will contain a list of cached things by cache key
      cache_values_by_key = {}

      cache_to_populate = CacheKeysToPopulate.new
      block.call(needed_keys.to_set(), cache_to_populate)

      cache_to_populate.cache_keys_to_populate.each do |k, v|
        cache_values_by_key[k] = v
        self.cache_adapter.setex(k, cache_expiry, v)
      end

      return cache_values_by_key
    end

    return fetch_multi(keys, cache_expiry, &block)
  end

  # Allows you to fetch multiple keys from the remote cache to populate them.
  # If they are not in the cache, this method will yield with the needed_keys and an object you call
  # #write(cache_key, value) on to be populated back into the cache.
  #
  # This method should be called with a block that takes in the needed cache keys and an object that supports
  # #write(key, value) to write the data back. E.g.,
  #
  # CacheAdapter.fetch_multi(["key1", "key2"]) do |needed_cache_keys, caches|
  #   if needed_cache_keys.include?("key1")
  #     caches.write("key1", compute_value_for_key1())
  #   end
  #
  #   if needed_cache_keys.include?("key2")
  #     caches.write("key2", compute_value_for_key2())
  #   end
  # end
  #
  # @param [Array<String>] keys Array of String cache keys
  # @param [Integer] cache_expiry Optional integer of how long to expire the items in the Cache
  def self.fetch_multi(keys, cache_expiry = nil)
    cache_expiry ||= DEFAULT_EXPIRE_IN

    # Will contain a list of cached things by cache key
    cache_values_by_key = {}

    remote_cache_result = Appboy::Cache.read_multi(keys)
    remote_cache_result.each do |k, v|
      if !cache_miss?(v, k, cache_miss_predicate)
        cache_values_by_key[k] = v
        self.cache_adapter.setex(k, cache_expiry, v)
      end
    end

    needed_keys = keys - cache_values_by_key.keys
    if !needed_keys.empty? && block_given?
      cache_to_populate = CacheKeysToPopulate.new
      yield needed_keys.to_set(), cache_to_populate

      cache_to_populate.cache_keys_to_populate.each do |k, v|
        cache_values_by_key[k] = v
        self.cache_adapter.setex(k, cache_expiry, v)
      end
    end

    return cache_values_by_key
  end

  def self.redis
    return @redis ||= ConnectionPool.new(:size => 2, :timeout => 5) do
      Redis.new(ENV["REDIS_URL"])
    end
  end

  def self.cache_adapter
    return @cache_adapter ||= ActiveSupport::Cache::MemCacheStore.new(ENV["MEMCACHED_CLUSTER_URLS"])
  end

  class CacheKeysToPopulate
    attr_reader :cache_keys_to_populate

    def initialize
      @cache_keys_to_populate = {}
    end

    def write(key, value)
      @cache_keys_to_populate[key] = value
    end
  end
end
