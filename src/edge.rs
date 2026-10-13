//! Bounded single-producer, single-consumer edge used by the threaded
//! driver for `buffer(capacity)`.
//!
//! `std::sync::mpsc::sync_channel(capacity)` allocates every slot up
//! front, so a large capacity costs memory (or aborts the process) before
//! a single item flows. This edge instead uses the unbounded `mpsc`
//! channel, whose storage grows in small blocks as items arrive and is
//! freed as they are consumed, and enforces the bound with a counter:
//!
//! * the sender counts what it sent (`sent`, private to the sender);
//! * the receiver publishes what it took (`received`, shared);
//! * a send waits while `sent - received == capacity`, and then until a
//!   quarter of the capacity is free, so a fast sender hands over items in
//!   runs rather than one handshake per item. A capacity of 0 is a
//!   hand-off: the sender waits after each send until the receiver has
//!   taken the message.
//!
//! The sender caches the last `received` it read and only re-reads the
//! shared counter when the cache says the edge is full; the receiver
//! publishes its count every `max(capacity / 16, 1)` messages and always
//! before it blocks waiting for more. So the counter's cache line is not
//! bounced between the two threads on every item. A published count is
//! never ahead of the truth, so the bound always holds. Waiting spins
//! briefly, then yields, then blocks on a condition variable; the
//! receiver signals it only when a sender is blocked.

use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;
use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};
use std::sync::{Arc, Condvar, Mutex, PoisonError};

/// Largest accepted `buffer` capacity. Beyond this an edge no longer
/// provides meaningful backpressure, and a value this large is almost
/// always a mistake.
pub(crate) const MAX_CAPACITY: usize = 1 << 20;

/// Keeps the receiver's counter on its own cache line.
#[derive(Debug, Default)]
#[repr(align(128))]
struct Padded(AtomicU64);

/// State shared by the two ends of one edge.
#[derive(Debug, Default)]
struct Gate {
    /// Messages the receiver has taken off the channel (published in
    /// batches; never ahead of the true count).
    received: Padded,
    /// True while the sender is blocked (or about to block) on `wake`.
    waiting: AtomicBool,
    /// True once the receiver is gone.
    closed: AtomicBool,
    lock: Mutex<()>,
    wake: Condvar,
}

impl Gate {
    /// Wake a blocked sender. Called after `received` or `closed` changed.
    fn notify(&self) {
        if self.waiting.load(Ordering::SeqCst) {
            // Taking the lock orders this notification after the sender's
            // final check, so the wake-up cannot be lost.
            let _guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
            self.wake.notify_one();
        }
    }
}

/// Why a send failed: the receiving side is gone. Carries the message
/// back, like `mpsc::SendError`.
#[derive(Debug)]
pub(crate) struct Disconnected<T>(pub(crate) T);

/// Sending side of an edge.
#[derive(Debug)]
pub(crate) struct EdgeSender<T> {
    tx: mpsc::Sender<T>,
    gate: Arc<Gate>,
    capacity: u64,
    /// Messages sent so far.
    sent: u64,
    /// Last value of `gate.received` this side has seen.
    seen: u64,
}

/// Receiving side of an edge.
#[derive(Debug)]
pub(crate) struct EdgeReceiver<T> {
    rx: mpsc::Receiver<T>,
    gate: Arc<Gate>,
    /// Messages taken so far.
    received: u64,
    /// Last count stored in `gate.received`.
    published: u64,
    /// Publish after this many messages (1 for a hand-off edge).
    batch: u64,
}

/// Create an edge holding at most `capacity` messages (0 = hand-off).
pub(crate) fn edge<T>(capacity: usize) -> (EdgeSender<T>, EdgeReceiver<T>) {
    let (tx, rx) = mpsc::channel();
    let gate = Arc::new(Gate::default());
    let capacity = u64::try_from(capacity).unwrap_or(u64::MAX);
    let sender = EdgeSender {
        tx,
        gate: Arc::clone(&gate),
        capacity,
        sent: 0,
        seen: 0,
    };
    let receiver = EdgeReceiver {
        rx,
        gate,
        received: 0,
        published: 0,
        batch: (capacity / 16).max(1),
    };
    (sender, receiver)
}

impl<T> EdgeSender<T> {
    /// Messages sent but not yet taken by the receiver, as far as this
    /// side knows (refreshes the cached counter first).
    fn in_flight(&mut self) -> u64 {
        self.seen = self.gate.received.0.load(Ordering::Acquire);
        self.sent.wrapping_sub(self.seen)
    }

    /// True if one more message fits without waiting.
    fn has_room(&mut self) -> bool {
        // A capacity of 0 still lets one message in; the hand-off wait
        // happens after the send.
        let limit = self.capacity.max(1);
        self.sent.wrapping_sub(self.seen) < limit || self.in_flight() < limit
    }

    /// Wait until `received >= target` or the receiver is gone. Returns
    /// false if the receiver is gone.
    fn wait_until(&mut self, target: u64) -> bool {
        let reached = |gate: &Gate| gate.received.0.load(Ordering::Acquire) >= target;
        for round in 0_u32..96 {
            if reached(&self.gate) {
                return true;
            }
            if self.gate.closed.load(Ordering::Acquire) {
                return false;
            }
            if round < 64 {
                core::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
        let gate = &*self.gate;
        let mut guard = gate.lock.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            gate.waiting.store(true, Ordering::SeqCst);
            // Re-check after announcing the wait: the receiver either sees
            // `waiting` and notifies under the lock, or this sees its update.
            if gate.received.0.load(Ordering::SeqCst) >= target {
                gate.waiting.store(false, Ordering::SeqCst);
                return true;
            }
            if gate.closed.load(Ordering::SeqCst) {
                gate.waiting.store(false, Ordering::SeqCst);
                return false;
            }
            guard = gate
                .wake
                .wait(guard)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn push(&mut self, msg: T) -> Result<(), Disconnected<T>> {
        match self.tx.send(msg) {
            Ok(()) => {
                self.sent = self.sent.wrapping_add(1);
                Ok(())
            }
            Err(mpsc::SendError(msg)) => Err(Disconnected(msg)),
        }
    }

    /// Send, waiting while the edge is full (and, for capacity 0, until
    /// the receiver has taken the message).
    pub(crate) fn send(&mut self, msg: T) -> Result<(), Disconnected<T>> {
        if !self.has_room() {
            // Room for one more once `sent - received < max(capacity, 1)`.
            // Wait until a quarter of the edge is free rather than a single
            // slot, so a sender that outpaces its receiver hands over items
            // in runs instead of one handshake per item.
            let limit = self.capacity.max(1);
            let room = (limit / 4).max(1);
            let target = self.sent.saturating_sub(limit).saturating_add(room);
            if !self.wait_until(target) {
                return Err(Disconnected(msg));
            }
        }
        self.push(msg)?;
        if self.capacity == 0 {
            let target = self.sent;
            // The message is in the channel; if the receiver vanished it
            // will never be taken, which the caller learns on its next send.
            let _taken = self.wait_until(target);
        }
        Ok(())
    }

    /// Send only if there is room right now. `Ok(false)` means the edge
    /// was full and the message was dropped.
    pub(crate) fn try_send(&mut self, msg: T) -> Result<bool, Disconnected<T>> {
        if self.gate.closed.load(Ordering::Acquire) {
            return Err(Disconnected(msg));
        }
        if self.capacity == 0 || !self.has_room() {
            return Ok(false);
        }
        self.push(msg).map(|()| true)
    }
}

impl<T> EdgeReceiver<T> {
    /// Store the count of taken messages and wake a blocked sender.
    fn publish(&mut self) {
        if self.published != self.received {
            self.published = self.received;
            self.gate.received.0.store(self.received, Ordering::SeqCst);
            self.gate.notify();
        }
    }

    fn took(&mut self, msg: T) -> T {
        self.received = self.received.wrapping_add(1);
        if self.received.wrapping_sub(self.published) >= self.batch {
            self.publish();
        }
        msg
    }

    /// Wait for the next message; `None` once the sender is gone and the
    /// edge is drained.
    pub(crate) fn recv(&mut self) -> Option<T> {
        match self.rx.try_recv() {
            Ok(msg) => return Some(self.took(msg)),
            Err(TryRecvError::Disconnected) => return None,
            Err(TryRecvError::Empty) => {}
        }
        // About to block: let a waiting sender see everything taken.
        self.publish();
        let msg = self.rx.recv().ok()?;
        Some(self.took(msg))
    }

    /// Wait up to `timeout`. `Err(true)` = timed out, `Err(false)` = the
    /// sender is gone and the edge is drained.
    pub(crate) fn recv_timeout(&mut self, timeout: Duration) -> Result<T, bool> {
        match self.rx.try_recv() {
            Ok(msg) => return Ok(self.took(msg)),
            Err(TryRecvError::Disconnected) => return Err(false),
            Err(TryRecvError::Empty) => {}
        }
        self.publish();
        match self.rx.recv_timeout(timeout) {
            Ok(msg) => Ok(self.took(msg)),
            Err(RecvTimeoutError::Timeout) => Err(true),
            Err(RecvTimeoutError::Disconnected) => Err(false),
        }
    }
}

impl<T> Drop for EdgeReceiver<T> {
    fn drop(&mut self) {
        self.gate.closed.store(true, Ordering::SeqCst);
        self.gate.notify();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn test_order_and_bound_hold_under_contention() {
        for capacity in [0_usize, 1, 2, 7, 64] {
            let (mut tx, mut rx) = edge::<u64>(capacity);
            let gate = Arc::clone(&tx.gate);
            let producer = thread::spawn(move || {
                for n in 0..20_000_u64 {
                    tx.send(n).unwrap();
                    // Never more than `capacity` (at least one) in flight.
                    let taken = gate.received.0.load(Ordering::SeqCst);
                    assert!(n + 1 - taken <= capacity.max(1) as u64, "cap {capacity}");
                }
            });
            for expected in 0..20_000_u64 {
                assert_eq!(rx.recv(), Some(expected));
            }
            assert_eq!(rx.recv(), None);
            producer.join().unwrap();
        }
    }

    #[test]
    fn test_capacity_zero_waits_for_the_receiver() {
        let (mut tx, mut rx) = edge::<u8>(0);
        let started = Instant::now();
        let consumer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            rx.recv()
        });
        tx.send(1).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(40));
        assert_eq!(consumer.join().unwrap(), Some(1));
    }

    #[test]
    fn test_blocked_sender_wakes_when_the_receiver_goes_away() {
        let (mut tx, rx) = edge::<u8>(1);
        tx.send(1).unwrap();
        let dropper = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            drop(rx);
        });
        assert!(matches!(tx.send(2), Err(Disconnected(2))));
        dropper.join().unwrap();
        assert!(matches!(tx.try_send(3), Err(Disconnected(3))));
    }

    #[test]
    fn test_hand_off_sender_returns_when_the_receiver_goes_away() {
        let (mut tx, rx) = edge::<u8>(0);
        let dropper = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            drop(rx);
        });
        // The message was accepted by the channel; the wait ends when the
        // receiver is dropped, and the next send reports the disconnect.
        tx.send(1).unwrap();
        dropper.join().unwrap();
        assert!(tx.send(2).is_err());
    }

    #[test]
    fn test_try_send_drops_when_full() {
        let (mut tx, mut rx) = edge::<u8>(2);
        assert!(tx.try_send(1).unwrap());
        assert!(tx.try_send(2).unwrap());
        assert!(!tx.try_send(3).unwrap());
        assert_eq!(rx.recv(), Some(1));
        assert!(tx.try_send(4).unwrap());
        assert_eq!(rx.recv(), Some(2));
        assert_eq!(rx.recv(), Some(4));
        let (mut zero, _rx) = edge::<u8>(0);
        assert!(!zero.try_send(1).unwrap());
    }

    #[test]
    fn test_recv_timeout_reports_timeout_and_disconnect() {
        let (tx, mut rx) = edge::<u8>(4);
        assert_eq!(rx.recv_timeout(Duration::from_millis(5)), Err(true));
        drop(tx);
        assert_eq!(rx.recv_timeout(Duration::from_millis(5)), Err(false));
    }

    #[test]
    fn test_large_capacity_allocates_lazily() {
        // A capacity this large would reserve gigabytes with a
        // preallocating channel; here it costs nothing until used.
        let (mut tx, mut rx) = edge::<[u8; 64]>(MAX_CAPACITY);
        let started = Instant::now();
        tx.send([1; 64]).unwrap();
        assert_eq!(rx.recv(), Some([1; 64]));
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
