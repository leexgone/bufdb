use std::sync::Arc;
use std::sync::RwLock;
use std::thread::JoinHandle;
use std::thread::spawn;
use std::time::Duration;

pub trait Maintainable : Send + Sync {
    fn maintain(&self);
}

struct DaemonData<T: Maintainable + 'static> {
    items: Vec<Arc<T>>,
    thread: Option<JoinHandle<()>>,
    terminated: bool,
    interval: Duration,
}

impl <T: Maintainable> Maintainable for DaemonData<T> {
    fn maintain(&self) {
        todo!()
    }
}

impl <T: Maintainable> DaemonData<T> {
    fn new() -> Self {
        Self { 
            items: Vec::new(), 
            thread: None, 
            terminated: false,
            interval: Duration::from_secs(60),
        }
    }
}

pub struct Daemon<T: Maintainable + 'static> {
    data: Arc<RwLock<DaemonData<T>>>
}

impl <T: Maintainable> Daemon<T> {
    pub fn new() -> Self {
        Self { 
            data: Arc::new(RwLock::new(DaemonData::new()))
        }
    }

    pub fn add(&self, item: Arc<T>) {
        let mut data = self.data.write().unwrap();
        data.items.push(item);

        if data.thread.is_none() {
            data.terminated = false;

            let local_data = self.data.clone();
            let thread = spawn(move || {
                // let data = data.read().unwrap();
                let data = local_data.write().unwrap();
                for item in data.items.iter() {
                    item.maintain();
                }
            });

            data.thread = Some(thread);
        }
    }

}