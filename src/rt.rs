use std::marker::PhantomData;
use std::sync::mpsc::Sender;
use std::thread::{JoinHandle, spawn};

/// This implements a toy share nothing/thread per core sharded execution strategy where items of
/// type `T` are submitted to a thread pool for processing. The shard selection is defined by the
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
/// this strategy may not be optimal, and a work stealing scheduler may be more appropriate.
///
/// This implementation does not handle back pressure. It could see queues for one shard be
/// significantly longer than queues for other shards if the number of transactions per client is
/// statistically uneven, or all queues fill up if reading is faster than processing. There are
/// multiple strategies to handle this, which could be a topic for discussion.
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
    /// ```rust
    /// Creates a new instance of the struct that manages parallelism by processing data
    /// across a specified number of worker threads, each pinned to its own CPU core.
    ///
    /// # Parameters
    /// - `parallelism`: The number of worker threads to spawn. Each thread will be pinned to a different CPU core.
    ///   The value must not exceed the number of available CPU cores to avoid thread contention.
    /// - `func`: A closure or function that takes mutable access to a state object of type `S` and processes an
    ///   incoming item. This function is invoked for each item received in the thread's input queue.
    ///
    /// # Type Parameters
    /// - `F`: The type of the function or closure passed in `func`.
    /// - `S`: The state object type to be used and modified within each worker thread. Must implement the `Default`
    ///   trait for initialization.
    ///
    /// # Returns
    /// An instance of the struct containing worker threads, each associated with:
    /// - A transmission channel to send tasks into the thread.
    /// - A join handle to track the lifecycle of the thread.
    ///
    /// # Implementation Details
    /// - The method determines the available CPU cores using `core_affinity::get_core_ids()` and assigns threads
    ///   to specific cores using `core_affinity::set_for_current(core_id)`. This ensures better cache locality and
    ///   reduces thread contention.
    /// - A `Vec` of capacity `parallelism` is used to store the tuple `(tx, join_handle)` for each worker thread:
    ///   - `tx`: Sender end of the mpsc (multi-producer, single-consumer) channel for dispatching tasks to the thread.
    ///   - `join_handle`: A `JoinHandle` for the thread, which can be used to wait for its completion or retrieve
    ///     its final state.
    /// - Each worker thread initializes its own state object using `S::default()`, and processes tasks by receiving
    ///   items from the channel and passing them to `func`.
    /// - When the channel closes, the thread exits, and its final state is returned (if joined).
    ///
    /// # Panics
    /// - The function panics if `core_affinity::get_core_ids()` fails to enumerate CPU cores.
    ///
    /// ```
    fn new(max_threads: u8, func: F) -> Self {
        let mut shards = Vec::with_capacity(max_threads as usize);
        // enumerate available cores
        for core_id in core_affinity::get_core_ids()
            .expect("Could not enumerate cores")
            .into_iter()
            .take(max_threads as usize)
        {
            let f = func.clone();
            // spsc would be better here, but let's keep our dependencies simple for this exercise
            let (tx, rx) = std::sync::mpsc::channel();
            let join_handle = spawn(move || {
                // lock the thread to a specific core
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

    /// ```rust
    /// Processes an item by determining its shard and sending it to the appropriate thread pool.
    ///
    /// # Parameters
    /// - `item: T` - The item to be processed, where `T` must implement `Shardable`.
    ///
    /// # Panics
    /// - This function will panic if:
    ///     - The `send` operation on the transmitter fails. This indicates a bug in the
    ///       program because the shard's thread pool should always be able to accept
    ///       new items unless an unexpected condition occurs.
    ///
    /// ```
    fn process_item(&self, item: T) {
        let shard_id = item.shard_id(self.shards.len() as u8);
        let (tx, _) = &self.shards[shard_id];
        tx.send(item).expect("Could not submit item to thread pool"); // this would be a bug
    }

    /// ```rust
    /// Finalizes the current operation and collects the results from all shards.
    ///
    /// This method processes each shard by performing the following steps:
    /// 1. Drops the sender (`tx`) associated with each shard. This ensures that the receiver (`recv`)
    ///    of the corresponding shard will return an error, signaling the thread to exit its processing loop.
    /// 2. Joins the thread handle (`join_handle`) associated with each shard to retrieve the final state
    ///    produced by that shard's thread. If the thread panics during execution, this method will
    ///    panic as well, as this indicates a bug in the threading logic or shard processing.
    ///
    /// # Returns
    /// A `Vec<S>` containing the final states of all shards after their respective threads have completed
    /// execution.
    ///
    /// ```
    fn finish(self) -> Vec<S> {
        let mut result = Vec::with_capacity(self.shards.len());
        for (tx, join_handle) in self.shards {
            // after dropping the sender, the recv method of `Receiver` will return an error, which
            // in turn will cause the shard thread to exit its loop and return the shard state. which
            // is collected via `JoinHandle::join` below.
            drop(tx);
            result.push(join_handle.join().expect("Thread panicked")); // this would be a bug
        }
        result
    }

    /// ```
    /// Consumes an iterator over `Result<T, E>` items and processes them in parallel using the
    /// specified number of worker threads by applying function `func` to each item.
    ///
    /// # Parameters
    /// - `parallelism`: The level of parallelism, specified as the number of concurrent workers to process items.
    /// - `func`: A closure or function that takes an input of type `T` and produces a transformed output of type `S`.
    /// - `items`: An iterator over `Result<T, E>` items, where `T` is the input type and `E` is the error type.
    ///
    /// # Returns
    /// - `Result<impl Iterator<Item = S>, E>`:
    ///   - On success, returns an iterator over the processed items of type `S`.
    ///   - On failure, if any item yields an error during processing, returns the first encountered error of type `E`.
    ///
    /// # Example
    /// ```
    /// let input = vec![
    ///     Ok(1),
    ///     Ok(2),
    ///     Ok(3),
    /// ];
    /// let result: Result<Vec<_>, _> = try_fold(4, |x| x * 2, input.into_iter())?
    ///     .collect();
    /// assert_eq!(result, vec![2, 4, 6]);
    /// ```
    ///
    /// # Errors
    /// If any item from the input iterator is an `Err`, processing will halt, and the first encountered error will be returned.
    ///
    /// # Notes
    /// - This function leverages internal parallelism to process items concurrently, based on the specified `parallelism` level.
    /// - All items must be valid (i.e., `Ok` variants of the `Result`) for the function to succeed.
    ///
    /// # Panics
    /// - The function may panic if the `parallelism` level is set to `0` or if other invariants of the internal processing mechanism are violated.
    /// ```
    pub fn try_fold<E>(
        max_threads: u8,
        func: F,
        items: impl Iterator<Item = Result<T, E>>,
    ) -> Result<impl Iterator<Item = S>, E> {
        let rt = Self::new(max_threads, func);
        for item in items {
            rt.process_item(item?)
        }
        Ok(rt.finish().into_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[test]
    fn test_runtime() {
        struct Item {
            id: u32,
            value: u32,
        }
        impl Shardable for Item {
            fn shard_id(&self, num_shards: u8) -> usize {
                self.id as usize % num_shards as usize
            }
        }

        let result = ShardedThreadPerCoreRuntime::<Item, _, [u32; 2]>::try_fold(
            4,
            |s, x| s[x.id as usize] += x.value,
            vec![
                Ok::<_, Infallible>(Item { id: 0, value: 1 }),
                Ok(Item { id: 1, value: 2 }),
                Ok(Item { id: 0, value: 3 }),
                Ok(Item { id: 1, value: 4 }),
            ]
            .into_iter(),
        )
        .into_iter()
        .flatten()
        .reduce(|a, b| [a[0] + b[0], a[1] + b[1]])
        .unwrap();
        assert_eq!(result, [4, 6]);
    }
}
