use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use bufdb_lib::config::CacheConfig;
use chrono::Local;

pub fn now() -> i64 {
    Local::now().timestamp_millis()
}

#[macro_export]
macro_rules! get_timestamp {
    ($atomic: expr) => {
        $atomic.load(std::sync::atomic::Ordering::Relaxed)
    };
}

#[macro_export]
macro_rules! set_timestamp {
    ($atomic: expr) => {
        $atomic.store(chrono::Local::now().timestamp_millis(), std::sync::atomic::Ordering::Relaxed)
    };
    ($atomic: expr, $value: expr) => {
        $atomic.store($atomic, std::sync::atomic::Ordering::Relaxed)
    }
}

pub trait Poolable {
    fn name(&self) -> &str;
    fn last_access(&self) -> i64;
    fn touch(&self);
}

#[derive(Debug, Default)]
pub struct CachePool<T: Poolable> {
    items: RwLock<HashMap<String, Arc<T>>>,
}

impl <T: Poolable> CachePool<T> {
    pub fn new() -> Self {
        Self { 
            items: Default::default(), 
        }
    }

    pub fn is_empty(&self) -> bool {
        let items = self.items.read().unwrap();
        items.is_empty()
    }

    pub fn len(&self) -> usize {
        let items = self.items.read().unwrap();
        items.len()
    }

    pub fn contains(&self, name: &str) -> bool {
        let items = self.items.read().unwrap();
        items.contains_key(name)
    }

    pub fn get(&self, name: &str) -> Option<Arc<T>> {
        let items = self.items.read().unwrap();
        items.get(name).map(|x| {
            x.touch();
            x.clone()
        })
    }

    pub fn put(&self, item: Arc<T>) {
        let mut items = self.items.write().unwrap();

        item.touch();
        items.insert(item.name().into(), item);
    }

    pub fn remove(&self, name: &str) -> Option<Arc<T>> {
        let mut items = self.items.write().unwrap();
        items.remove(name)
    }

    pub fn collect(&self) -> Vec<Arc<T>> {
        let items = self.items.read().unwrap();
        items.values().map(|x| x.clone()).collect()
    }

    pub fn cleanup<C: CacheConfig>(&self, config: &C) {
        let mut items = self.collect();
        if items.is_empty() {
            return;
        }

        let cur_time = now();

        if let Some(min_live) = config.min_live_time() {
            if !min_live.is_zero() {
                let min_live = min_live.as_millis() as i64;
                items.retain(|x| cur_time - x.last_access() > min_live);
                if items.is_empty() {
                    return;
                }
            }
        }

        if let Some(max_idle) = config.max_idle_time() {
            if !max_idle.is_zero() {
                let max_idle = max_idle.as_millis() as i64;
                items = items.into_iter().filter_map(|x| {
                    let delta = cur_time - x.last_access();
                    if delta > max_idle {
                        self.remove(x.name());
                        None
                    } else {
                        Some(x)
                    }
                }).collect();
                if items.is_empty() {
                    return;
                }                
            }
        }

        if let Some(max_cache) = config.max_cache() {
            if items.len() > max_cache {
                items.sort_by(|a, b| a.last_access().cmp(&b.last_access()));
                for item in items.iter().take(items.len() - max_cache) {
                    self.remove(item.name());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicI64;
    use std::thread;
    use std::time::Duration;

    use bufdb_lib::config::CacheConfig;

    use super::CachePool;
    use super::Poolable;
    use super::now;

    struct PoolItem {
        name: String,
        last_access: AtomicI64,
    }

    impl PoolItem {
        fn new<T: Into<String>>(name: T) -> Self {
            Self { 
                name: name.into(), 
                last_access: AtomicI64::new(now()),
            }
        }
    }

    impl Poolable for PoolItem {
        fn name(&self) -> &str {
            &self.name
        }

        fn last_access(&self) -> i64 {
            get_timestamp!(self.last_access)
        }

        fn touch(&self) {
            // self.last_access.store(now(), std::sync::atomic::Ordering::Relaxed);
            set_timestamp!(self.last_access);
        }
    }

    #[derive(Debug, Default)]
    struct PoolConfig {
        max_cache: Option<usize>,
        min_live_time: Option<Duration>,
        max_idle_time: Option<Duration>,
    }

    impl CacheConfig for PoolConfig {
        fn max_cache(&self) -> Option<usize> {
            self.max_cache
        }

        fn min_live_time(&self) -> Option<Duration> {
            self.min_live_time
        }

        fn max_idle_time(&self) -> Option<Duration> {
            self.max_idle_time
        }
    }

    #[test]
    fn test_cache() {
        let cache: Arc<CachePool<PoolItem>> = Arc::new(CachePool::new());

        let mut handles = Vec::new();
        for i in 1..=5 {
            let id = i;
            let cache = cache.clone();
            let handle = thread::spawn(move || {
                thread::sleep(Duration::from_millis(id as _));

                let name = format!("OBJ-{}", id);
                cache.put(Arc::new(PoolItem::new(name)));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        cache.put(Arc::new(PoolItem::new("OBJ-0")));

        assert_eq!(6, cache.len());

        cache.cleanup(&PoolConfig {
            max_cache: Some(4),
            min_live_time: None,
            max_idle_time: None,
        });
        assert_eq!(4, cache.len());

        thread::sleep(Duration::from_millis(60));

        cache.cleanup(&PoolConfig {
            max_cache: Some(2),
            min_live_time: Some(Duration::from_secs(600)),
            max_idle_time: Some(Duration::from_millis(50)),
        });
        assert_eq!(4, cache.len());

        let item = cache.get("OBJ-0").unwrap();
        assert_eq!("OBJ-0", &item.name);

        cache.cleanup(&PoolConfig {
            max_cache: None,
            min_live_time: None,
            max_idle_time: Some(Duration::from_millis(50)),
        });

        assert_eq!(1, cache.len());
    }
}