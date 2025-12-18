use std::marker::PhantomData;
use std::sync::mpsc::{SendError, Sender};
use std::thread::{JoinHandle, spawn};

/// This implements a share nothing/thread per core sharded execution strategy where items of type
/// `T` are submitted to a thread pool for processing. The shard selection is defined by the
/// `Shardable` trait, which submitted items must implement. All shards have an instance of type `S`
/// which is the mutable state of the shard.
///
/// For this exercise, the Client ID is used for shard selection, and state `S` is an instance of
/// `crate::account::Accounts`
///
/// This strategy allows the dataset to be processed in parallel, while items per client are
/// processed in order, all while remaining lock-free and with minimal context switching.
///
/// My assumption is that the number of clients is high, and the number of transactions is high,
/// and because of this the shards should get a similar number of tasks. On smaller workloads
/// this strategy may not be optimal, and a work stealing strategy may be more appropriate.
///
/// # Types
/// - `T` is the type that will be submitted for processing
/// - `F` is a function of type (&mut S, T) which is run on the thread pool to fold `T` into `S`
/// - `S` is the mutable state of a shard
pub struct ShardedThreadPerCoreRuntime<T, F, S> {
    shards: Vec<(Sender<T>, JoinHandle<S>)>,
    _t: PhantomData<T>,
    _f: PhantomData<F>,
    _s: PhantomData<S>,
}

/// Allows a type to select which shard it should be submitted to.
pub trait Shardable {
    fn shard_id(&self, num_shards: u8) -> usize;
}

impl<T, F, S> ShardedThreadPerCoreRuntime<T, F, S>
where
    T: Send + Shardable + 'static,
    F: Fn(&mut S, T) + Clone + Send + 'static,
    S: Default + Send + 'static,
{
    pub fn new(parallelism: u8, func: F) -> Self {
        let mut shards = Vec::with_capacity(parallelism as usize);
        for core_id in core_affinity::get_core_ids()
            .expect("Could not enumerate cores")
            .into_iter()
            .take(parallelism as usize)
        {
            let f = func.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            let join_handle = spawn(move || {
                core_affinity::set_for_current(core_id);
                let mut state = S::default();
                while let Ok(item) = rx.recv() {
                    f(&mut state, item);
                }
                state
            });
            shards.push((tx, join_handle));
        }
        Self {
            shards,
            _t: PhantomData,
            _f: PhantomData,
            _s: PhantomData,
        }
    }

    pub fn process_item(&self, item: T) -> Result<(), SendError<T>> {
        let shard_id = item.shard_id(self.shards.len() as u8);
        let (tx, _) = &self.shards[shard_id];
        tx.send(item)
    }

    pub fn finish(self) -> Vec<S> {
        let mut result = Vec::with_capacity(self.shards.len());
        for (tx, join_handle) in self.shards {
            drop(tx);
            result.push(join_handle.join().expect("Thread panicked"));
        }
        result
    }
}
