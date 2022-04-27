// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{BinaryHeap, HashMap},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Schedule {
    deadline: Instant,
    task_id: usize,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum TaskKind {
    /// The task is always executed at the specified interval.
    Interval(Duration),
    /// The task will be fired when deadline are reached.
    Deadline(Instant),
    /// The task will be fired after the specified interval.
    After(Duration),
}

struct Task {
    kind: TaskKind,
    data: *mut (),
    arg: *mut (),
    method: *const (),
}

impl Task {
    fn build<T>(kind: TaskKind, data: &mut T, method: fn(&mut T)) -> Self {
        debug_assert_eq!(
            std::mem::size_of::<fn(&mut T)>(),
            std::mem::size_of::<*const ()>()
        );
        debug_assert_eq!(
            std::mem::size_of::<fn(&mut T)>(),
            std::mem::size_of::<fn(*mut ())>()
        );
        Task {
            kind,
            data: data as *mut _ as *mut (),
            arg: std::ptr::null_mut(),
            method: method as *const (),
        }
    }

    fn build_with_arg<T, A>(
        kind: TaskKind,
        data: &mut T,
        arg: &mut A,
        method: fn(&mut T, &mut A),
    ) -> Self {
        debug_assert_eq!(
            std::mem::size_of::<fn(&mut T, &mut A)>(),
            std::mem::size_of::<*const ()>()
        );
        debug_assert_eq!(
            std::mem::size_of::<fn(&mut T, &mut A)>(),
            std::mem::size_of::<fn(*mut (), &mut ())>()
        );
        Task {
            kind,
            data: data as *mut _ as *mut (),
            arg: arg as *mut _ as *mut (),
            method: method as *const (),
        }
    }
}

impl Task {
    fn invoke(&self) {
        use std::mem::transmute;

        // SAFETY:
        // 1. The target platform allows this conversion to do type punning, so will Rust too.
        // 2. The only way to construct a `Task` is to call `Timer::add` and `Timer::add_with_arg`,
        // both of which guarantee `data` and `arg` lifetimes outlive to `Timer`.
        unsafe {
            if self.arg.is_null() {
                let method = transmute::<_, fn(*mut ())>(self.method);
                (method)(self.data);
            } else {
                let method = transmute::<_, fn(*mut (), *mut ())>(self.method);
                (method)(self.data, self.arg);
            }
        }
    }
}

#[derive(Default)]
pub struct Timer {
    next_task_id: usize,
    schedules: BinaryHeap<Schedule>,
    tasks: HashMap<usize, Task>,
}

impl Timer {
    pub fn new() -> Self {
        Timer::default()
    }

    #[inline]
    pub fn duration_since_now(&self) -> Option<Duration> {
        self.schedules.peek().map(Schedule::left_duration_since_now)
    }

    pub fn add<'a, 'b, T>(
        &'b mut self,
        kind: TaskKind,
        callback: fn(&mut T),
        data: &'a mut T,
    ) -> usize
    where
        'b: 'a,
    {
        self.add_task(Task::build(kind, data, callback))
    }

    pub fn add_with_arg<'a, 'b, T, A>(
        &'b mut self,
        kind: TaskKind,
        callback: fn(&mut T, &mut A),
        data: &'a mut T,
        arg: &'a mut A,
    ) -> usize
    where
        'b: 'a,
    {
        self.add_task(Task::build_with_arg(kind, data, arg, callback))
    }

    #[inline]
    #[allow(dead_code)]
    pub fn remove(&mut self, task_id: usize) {
        self.tasks.remove(&task_id);
    }

    pub fn dispatch_tasks(&mut self) {
        let now = Instant::now();
        while let Some(schedule) = self.schedules.peek() {
            if schedule.deadline > now {
                break;
            }

            let schedule = self.schedules.pop().expect("schedule");
            self.on_schedule(schedule);
        }
    }

    fn on_schedule(&mut self, schedule: Schedule) {
        if let Some(task) = self.tasks.get(&schedule.task_id) {
            task.invoke();
            if let TaskKind::Interval(duration) = task.kind {
                let deadline = schedule.deadline + duration;
                let next_schedule = Schedule {
                    deadline,
                    task_id: schedule.task_id,
                };
                self.schedules.push(next_schedule);
            } else {
                self.tasks.remove(&schedule.task_id);
            }
        }
    }

    fn add_task(&mut self, task: Task) -> usize {
        let task_id = self.next_task_id;
        let schedule = task.schedule(task_id);
        self.next_task_id += 1;
        self.tasks.insert(task_id, task);
        self.schedules.push(schedule);
        task_id
    }
}

impl Schedule {
    #[inline]
    fn left_duration_since_now(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

impl Task {
    fn schedule(&self, task_id: usize) -> Schedule {
        let deadline = match self.kind {
            TaskKind::Deadline(instant) => instant,
            TaskKind::After(duration) => Instant::now() + duration,
            TaskKind::Interval(duration) => Instant::now() + duration,
        };
        Schedule { deadline, task_id }
    }
}
