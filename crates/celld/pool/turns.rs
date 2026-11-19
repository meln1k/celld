// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Turn scheduling: which waiter enters an isolate next.
//!
//! An isolate runs one turn at a time. Each turn waits in a lane, the
//! stateless lane or one per cell, and the scheduler hands the turn to the
//! lanes in round robin. A turn is held by a `TurnPermit`, and every way a
//! waiter can end, with or without its grant, hands the turn on.
//!
//! An op is polled only by its event's driver, and only between that driver's
//! turns. So an op must never wait for a turn itself: the scheduler could
//! grant it the turn while its driver waits for a turn of its own, and then
//! nothing polls the op and every cell of the isolate stalls. Such an op runs
//! on a task of its own (`asyncrt::enqueue_on_own_task`). The driver polls
//! each op as a `TurnlessOp`, and `Slot::turn_in` refuses a turn inside one
//! in every build with debug assertions, whatever the timing.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum TurnLane {
    Stateless,
    Cell(String),
}

#[derive(Default)]
struct TurnSchedule {
    current: Option<TurnLane>,
    ready: std::collections::VecDeque<TurnLane>,
    waiting: std::collections::HashMap<
        TurnLane,
        std::collections::VecDeque<tokio::sync::oneshot::Sender<()>>,
    >,
}

#[derive(Default)]
pub(super) struct TurnScheduler {
    schedule: Mutex<TurnSchedule>,
}

pub(super) struct TurnPermit<'a>(&'a TurnScheduler);

/// A waiter for a turn. Dropped before its grant, it leaves a closed sender
/// in its lane, which `release` skips. Dropped after `release` granted it the
/// turn but before it returned a permit, it would hold the turn with no permit
/// to give it back, so it hands the turn on instead.
struct Waiting<'a> {
    scheduler: &'a TurnScheduler,
    receive: tokio::sync::oneshot::Receiver<()>,
    granted: bool,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if self.granted {
            return;
        }
        // `release` sends under the schedule's lock, so either the send came
        // first and is read here, or it fails and `release` skips this lane.
        self.receive.close();
        if self.receive.try_recv().is_ok() {
            self.scheduler.release();
        }
    }
}

impl TurnScheduler {
    pub(super) async fn acquire(&self, lane: TurnLane) -> TurnPermit<'_> {
        let receive = {
            let mut schedule = self.schedule.lock().expect("turn scheduler poisoned");
            if schedule.current.is_none() {
                schedule.current = Some(lane);
                None
            } else {
                let (send, receive) = tokio::sync::oneshot::channel();
                if !schedule.waiting.contains_key(&lane) && schedule.current.as_ref() != Some(&lane)
                {
                    schedule.ready.push_back(lane.clone());
                }
                schedule.waiting.entry(lane).or_default().push_back(send);
                Some(receive)
            }
        };
        if let Some(receive) = receive {
            let mut waiting = Waiting {
                scheduler: self,
                receive,
                granted: false,
            };
            (&mut waiting.receive)
                .await
                .expect("the scheduler sends each waiter its turn or skips its closed lane");
            waiting.granted = true;
        }
        TurnPermit(self)
    }

    fn release(&self) {
        let mut schedule = self.schedule.lock().expect("turn scheduler poisoned");
        let current = schedule
            .current
            .take()
            .expect("a released turn scheduler has a current lane");
        if schedule
            .waiting
            .get(&current)
            .is_some_and(|waiters| !waiters.is_empty())
        {
            schedule.ready.push_back(current);
        }
        while let Some(lane) = schedule.ready.pop_front() {
            let (send, keep_lane) = {
                let waiters = schedule
                    .waiting
                    .get_mut(&lane)
                    .expect("a ready turn lane has waiters");
                (waiters.pop_front().unwrap(), !waiters.is_empty())
            };
            if !keep_lane {
                schedule.waiting.remove(&lane);
            }
            if send.send(()).is_ok() {
                schedule.current = Some(lane);
                return;
            }
            if keep_lane {
                schedule.ready.push_back(lane);
            }
        }
    }
}

impl Drop for TurnPermit<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[cfg(debug_assertions)]
thread_local! {
    static POLLING_TURNLESS_OP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// An op as its event's driver polls it, which must not wait for a turn.
pub(crate) struct TurnlessOp<F>(pub(crate) F);

impl<F: Future + Unpin> Future for TurnlessOp<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        #[cfg(debug_assertions)]
        let _restore = {
            struct Restore(bool);
            impl Drop for Restore {
                fn drop(&mut self) {
                    POLLING_TURNLESS_OP.set(self.0);
                }
            }
            Restore(POLLING_TURNLESS_OP.replace(true))
        };
        Pin::new(&mut self.0).poll(context)
    }
}

/// Refuse a turn taken while a driver polls an op, in builds with debug
/// assertions.
pub(super) fn refuse_turn_in_turnless_op() {
    #[cfg(debug_assertions)]
    assert!(
        !POLLING_TURNLESS_OP.get(),
        "an op polled by its event's driver waits for a turn: run it with asyncrt::enqueue_on_own_task"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        let mut context = Context::from_waker(std::task::Waker::noop());
        future.poll_unpin(&mut context)
    }

    #[test]
    fn a_waiter_dropped_after_its_grant_hands_the_turn_on() {
        let scheduler = TurnScheduler::default();
        let running = scheduler
            .acquire(TurnLane::Stateless)
            .now_or_never()
            .expect("an idle scheduler grants the first turn at once");
        let mut granted = Box::pin(scheduler.acquire(TurnLane::Cell("a".into())));
        let mut next = Box::pin(scheduler.acquire(TurnLane::Cell("b".into())));
        assert!(poll_once(&mut granted).is_pending());
        assert!(poll_once(&mut next).is_pending());

        // The turn goes to "a", whose waiter is dropped before it is polled
        // again, as an op its driver stops polling would be.
        drop(running);
        drop(granted);

        assert!(
            poll_once(&mut next).is_ready(),
            "the turn granted to the dropped waiter went to the next one"
        );
    }

    #[test]
    fn a_waiter_dropped_before_its_grant_is_skipped() {
        let scheduler = TurnScheduler::default();
        let running = scheduler
            .acquire(TurnLane::Stateless)
            .now_or_never()
            .expect("an idle scheduler grants the first turn at once");
        let mut dropped = Box::pin(scheduler.acquire(TurnLane::Cell("a".into())));
        let mut next = Box::pin(scheduler.acquire(TurnLane::Cell("b".into())));
        assert!(poll_once(&mut dropped).is_pending());
        assert!(poll_once(&mut next).is_pending());

        drop(dropped);
        drop(running);

        assert!(poll_once(&mut next).is_ready());
    }

    #[test]
    #[should_panic(expected = "an op polled by its event's driver waits for a turn")]
    fn a_turn_inside_a_turnless_op_is_refused() {
        let slot = crate::pool::Slot::vacant_for_request_cancellation_test();
        let mut op = TurnlessOp(Box::pin(slot.turn(|_| ())));
        let _ = poll_once(&mut op);
    }

    #[test]
    fn a_turn_after_a_turnless_op_is_allowed() {
        let mut op = TurnlessOp(Box::pin(async {}));
        assert!(poll_once(&mut op).is_ready());
        refuse_turn_in_turnless_op();
    }
}
