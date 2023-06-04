use std::fmt::Display;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use tempdir::TempDir;

use crate::error::Result;

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
    };
    ($t: ty as object) => {
        impl $t {
            pub fn readonly(&self) -> bool {
                self.readonly
            }
        
            pub fn set_readonly(mut self, readonly: bool) -> Self {
                self.readonly = readonly;
                self
            }
        
            pub fn temporary(&self) -> bool {
                self.temporary
            }
        
            pub fn set_temporary(mut self, temporary: bool) -> Self {
                self.temporary = temporary;
                self
            }
        }                
    };
}

macro_rules! write_cache_config {
    ($f: expr, $config: expr) => {
        if let Some(max_cache) = $config.max_cache {
            write!($f, ", max_cache = {}", max_cache)?;
        }

        if let Some(min_live_time) = $config.min_live_time {
            write!($f, ", min_live_time = {}", min_live_time.as_millis())?;
        }

        if let Some(max_idle_time) = $config.max_idle_time {
            write!($f, ", max_idle_time = {}", max_idle_time.as_millis())?;
        }
    };
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

    pub fn new_temp() -> Result<Self> {
        let dir = TempDir::new("BUF_")?;
        Ok(Self::new(dir.into_path()))
    }

    pub fn dir(&self) -> &Path {
        self.dir.as_path()
    }
}

impl_cache_config!(InstanceConfig as setter);
impl_cache_config!(InstanceConfig as getter);

impl Display for InstanceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}", self.dir.to_string_lossy())?;
        write_cache_config!(f, self);
        write!(f, "]")
    }
}

#[derive(Debug, Clone, Default)]
pub struct SchemaConfig {
    readonly: bool,
    temporary: bool,
    max_cache: Option<usize>,
    min_live_time: Option<Duration>,
    max_idle_time: Option<Duration>,
}

impl SchemaConfig {
    pub fn new(readonly: bool, temporary: bool) -> Self {
        Self { 
            readonly, 
            temporary, 
            max_cache: None, 
            min_live_time: None, 
            max_idle_time: None 
        }
    }
}

impl_cache_config!(SchemaConfig as setter);
impl_cache_config!(SchemaConfig as getter);
impl_cache_config!(SchemaConfig as object);

impl Display for SchemaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[readonly = {}, temporary = {}", self.readonly, self.temporary)?;
        write_cache_config!(f, self);
        write!(f, "]")
    }
}

#[derive(Debug, Clone, Default)]
pub struct TableConfig {
    pub readonly: bool,
    pub temporary: bool
}

impl TableConfig {
    pub fn new(readonly: bool, temporary: bool) -> Self {
        Self { 
            readonly, 
            temporary
        }
    }
}

impl_cache_config!(TableConfig as object);

impl Display for TableConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[readonly = {}, temporary = {}]", self.readonly, self.temporary)
    }
}
