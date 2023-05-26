use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use chrono::Local;

pub fn now() -> i64 {
    Local::now().timestamp()
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
        $atomic.store(bufdb_storage::cache::now(), std::sync::atomic::Ordering::Relaxed)
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
pub struct CachePool<'a, T: Poolable> {
    items: RwLock<Vec<Arc<T>>>,
    index: RwLock<HashMap<&'a str, &'a Arc<T>>>,
}

impl <'a, T: Poolable> CachePool<'a, T> {
    pub fn new() -> Self {
        Self { 
            items: Default::default(), 
            index: Default::default(),
        }
    }

    pub fn get(&self, name: &str) -> Option<Arc<T>> {
        let index = self.index.read().unwrap();
        let item = index.get(name);
        item.map(|&x| {
            x.touch();
            x.clone()
        })
    }
}