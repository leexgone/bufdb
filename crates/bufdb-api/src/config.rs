use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

pub trait CacheConfig {
    fn max_cache(&self) -> Option<usize>;
    fn min_live_time(&self) -> Option<Duration>;
    fn max_idle_time(&self) -> Option<Duration>;
}

macro_rules! impl_cache_config {
    ($t: ty as setter) => {
        impl $t {
            pub fn set_max_cache(mut self, max_cache: Option<usize>) -> Self {
                self.max_cache = max_cache;
                self
            }

            pub fn set_min_live_time(mut self, min_live_time: Option<Duration>) -> Self {
                self.min_live_time = min_live_time;
                self
            }
        
            pub fn set_max_idle_time(mut self, max_idle_time: Option<Duration>) -> Self {
                self.max_idle_time = max_idle_time;
                self
            }                
        }
    };
    ($t: ty as getter) => {
        impl CacheConfig for $t {
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
    }
}

#[derive(Debug, Clone)]
pub struct InstanceConfig {
    dir: PathBuf,
    max_cache: Option<usize>,
    min_live_time: Option<Duration>,
    max_idle_time: Option<Duration>,
}

impl InstanceConfig {
    pub fn new<T: Into<PathBuf>>(dir: T) -> Self {
        Self { 
            dir: dir.into(), 
            max_cache: None, 
            min_live_time: None, 
            max_idle_time: None
        }
    }

    pub fn dir(&self) -> &Path {
        self.dir.as_path()
    }
}

impl_cache_config!(InstanceConfig as setter);
impl_cache_config!(InstanceConfig as getter);

#[derive(Debug, Clone, Default)]
pub struct SchemaConfig {
    max_cache: Option<usize>,
    min_live_time: Option<Duration>,
    max_idle_time: Option<Duration>,
}

impl_cache_config!(SchemaConfig as setter);
impl_cache_config!(SchemaConfig as getter);

#[derive(Debug, Clone, Default)]
pub struct TableConfig {
    pub readonly: bool,
    pub temporary: bool
}
