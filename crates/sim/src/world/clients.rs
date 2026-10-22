//! The client workload and its retries.

use super::*;

impl World {
    // -----------------------------------------------------------------------
    // Clients

    pub(super) fn client_new(&mut self) {
        if !self.workload {
            return;
        }
        let cell = match self.quiet_parity() {
            // Every other cell, those of the other parity.
            Some(quiet) => {
                let busy = (self.cells.len() + quiet) / 2;
                let index = 2 * self.work_rng.below(busy as u64) as usize + 1 - quiet;
                self.cells[index].clone()
            }
            None => self.cells[self.work_rng.below(self.cells.len() as u64) as usize].clone(),
        };
        let write = self.work_rng.chance(self.config.write_probability);
        let alarm = (write && self.work_rng.chance(self.config.alarm_probability)).then(|| {
            if self.work_rng.chance(0.2) {
                AlarmIntent::Clear
            } else {
                AlarmIntent::Set {
                    due: self.t + self.work_rng.range(200, 8_000),
                }
            }
        });
        let op = self.new_op(cell, write, alarm, self.t + CLIENT_TIMEOUT_MS, false, None);
        self.client_send(op);
        let (lo, hi) = self.config.request_gap_ms;
        let gap = self.work_rng.range(lo, hi);
        self.after(gap, What::ClientNew);
    }

    /// The parity of the cells the workload leaves alone now
    /// (`Stops::quiet`). It alternates each window until the faults heal.
    fn quiet_parity(&self) -> Option<usize> {
        const WINDOW_MS: u64 = 20_000;
        (self.config.stops.quiet && self.chaos && self.cells.len() > 1)
            .then_some((self.t / WINDOW_MS % 2) as usize)
    }

    pub(super) fn new_op(
        &mut self,
        cell: String,
        write: bool,
        alarm: Option<AlarmIntent>,
        deadline: Time,
        retry: bool,
        ingress: Option<usize>,
    ) -> u64 {
        let op = self.next_op;
        self.next_op += 1;
        self.ops.insert(
            op,
            ClientOp {
                cell,
                write,
                alarm,
                deadline,
                attempt: 0,
                done: false,
                retry,
                ingress,
                succeeded: false,
                dispatch: Dispatch::default(),
            },
        );
        op
    }

    pub(super) fn client_send(&mut self, op: u64) {
        let Some(entry) = self.ops.get_mut(&op) else {
            return;
        };
        if entry.done {
            return;
        }
        entry.attempt += 1;
        let attempt = entry.attempt;
        let ingress = entry.ingress;
        self.stats.requests += 1;
        let candidates: Vec<usize> = match ingress {
            Some(node) => vec![node],
            None => (0..self.nodes.len())
                .filter(|node| self.nodes[*node].proc.is_some())
                .collect(),
        };
        let target = self
            .work_rng
            .pick(&candidates)
            .and_then(|node| self.nodes[*node].proc);
        let timeout = if self.ops[&op].retry {
            5_000
        } else {
            CLIENT_TIMEOUT_MS
        };
        self.after(timeout, What::ClientTimeout { op, attempt });
        match target {
            Some(proc) => {
                let deadline = self.t + OPERATION_DEADLINE_MS;
                self.ops.get_mut(&op).unwrap().dispatch = Dispatch {
                    proc,
                    deadline,
                    ..Dispatch::default()
                };
                let request = self.next_request;
                self.next_request += 1;
                let delay = self.net_rng.range(1, 3);
                self.work(
                    proc,
                    delay,
                    Work::ClientArrive {
                        op,
                        attempt,
                        request,
                        epoch: None,
                    },
                );
            }
            None => self.attempt_failed(op, attempt),
        }
    }

    pub(super) fn client_arrive(
        &mut self,
        id: ProcId,
        op: u64,
        attempt: u32,
        request: RequestId,
        via: Option<Epoch>,
    ) {
        let Some(entry) = self.ops.get(&op) else {
            return;
        };
        if entry.done || entry.attempt != attempt {
            return;
        }
        if self.refuses(id, via.is_some()) {
            // The listener closed with the shutdown: nothing ran here.
            return self.attempt_failed(op, attempt);
        }
        let (cell, write, alarm) = (entry.cell.clone(), entry.write, entry.alarm);
        self.procs.get_mut(&id).unwrap().requests.insert(
            request,
            ReqCtx {
                op,
                attempt,
                cell: cell.clone(),
                epoch: None,
                via,
                write,
                alarm,
                committed: None,
                arm_gate: ArmGate::None,
                answer: Answer::Pending,
                aborted: false,
                activity: Activity::Running,
                replied: false,
            },
        );
        let event = if via == Some(0) {
            Event::CapacityRequestAt {
                request,
                cell,
                now_mono_ms: 0,
            }
        } else {
            Event::RequestAt {
                request,
                cell,
                now_mono_ms: 0,
            }
        };
        self.drive(id, event);
    }

    pub(super) fn client_timeout(&mut self, op: u64, attempt: u32) {
        let Some(entry) = self.ops.get(&op) else {
            return;
        };
        if entry.done || entry.attempt != attempt {
            return;
        }
        self.attempt_failed(op, attempt);
    }

    pub(super) fn attempt_failed(&mut self, op: u64, attempt: u32) {
        let t = self.t;
        let Some(entry) = self.ops.get_mut(&op) else {
            return;
        };
        if entry.done || entry.attempt != attempt {
            return;
        }
        if entry.retry && t < entry.deadline {
            // Bump the attempt so late answers to this one are ignored.
            entry.attempt += 1;
            let delay = self.work_rng.range(200, 500);
            self.after(delay, What::ClientRetry { op });
        } else {
            entry.done = true;
        }
    }

    pub(super) fn attempt_succeeded(&mut self, op: u64, attempt: u32) {
        if let Some(entry) = self.ops.get_mut(&op) {
            if !entry.done && entry.attempt == attempt {
                entry.done = true;
                entry.succeeded = true;
            }
        }
    }
}
