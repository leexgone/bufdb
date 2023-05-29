use std::sync::Arc;
use std::sync::RwLock;
use std::thread::JoinHandle;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;
use std::time::Instant;

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

impl <T: Maintainable + PartialEq> DaemonData<T> {
    pub fn new() -> Self {
        Self { 
            items: Vec::new(), 
            thread: None, 
            terminated: false,
            interval: Duration::from_secs(60),
        }
    }

    // pub fn interval(mut self, interval: Duration) -> Self {
    //     self.interval = interval;
    //     self
    // }
}

pub struct Daemon<T: Maintainable + 'static> {
    data: Arc<RwLock<DaemonData<T>>>
}

impl <T: Maintainable + PartialEq> Daemon<T> {
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
                let mut run_at = Instant::now();
                loop {
                    let interval = {
                        let data = local_data.read().unwrap();
                        data.interval
                    };
                    
                    while run_at.elapsed() < interval {
                        sleep(Duration::from_millis(100));
                        let data = local_data.read().unwrap();
                        if data.terminated {
                            return;
                        }
                    }

                    {
                        let data = local_data.read().unwrap();
                        for item in data.items.iter() {
                            item.maintain();
                        }
                    }

                    run_at = Instant::now();
                }
            });

            data.thread = Some(thread);
        }
    }

    pub fn remove(&self, item: &Arc<T>) {
        let mut data = self.data.write().unwrap();
        data.items.retain(|x| x != item);
    }
}