use std::{
    collections::VecDeque,
    num::NonZero,
    ops::{AddAssign, SubAssign},
};

#[derive(Debug)]
pub(crate) struct RingBuffer<T> {
    inner: VecDeque<T>,
    capacity: NonZero<usize>,
}

#[expect(unused)]
impl<T> RingBuffer<T> {
    #[must_use]
    pub(crate) fn new(capacity: NonZero<usize>) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity.get()),
            capacity,
        }
    }

    pub(crate) fn push(&mut self, item: T) {
        if self.is_full() {
            self.inner.pop_front();
        }

        self.inner.push_back(item);

        debug_assert!(self.inner.len() <= self.capacity.get());
    }

    pub(crate) fn pop(&mut self) -> Option<T> {
        self.inner.pop_front()
    }

    pub(crate) fn back_mut(&mut self) -> Option<&mut T> {
        self.inner.back_mut()
    }

    #[must_use]
    pub(crate) fn is_full(&self) -> bool {
        self.inner.len() == self.capacity.get()
    }

    #[must_use]
    pub(crate) fn iter(&self) -> std::collections::vec_deque::Iter<'_, T> {
        self.inner.iter()
    }

    #[must_use]
    pub(crate) fn capacity(&self) -> NonZero<usize> {
        self.capacity
    }
}

#[derive(Debug)]
pub(crate) struct SumRingBuffer<T> {
    inner: VecDeque<T>,
    capacity: NonZero<usize>,
    sum: T,
}

impl<T> SumRingBuffer<T>
where
    T: AddAssign + SubAssign + PartialEq + for<'a> std::iter::Sum<&'a T> + Copy + Default,
{
    #[must_use]
    pub(crate) fn new(capacity: NonZero<usize>) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity.get()),
            capacity,
            sum: T::default(),
        }
    }

    pub(crate) fn push(&mut self, item: T) {
        if self.is_full() {
            let front = self.inner.pop_front().expect("VecDeque is full, not empty");
            self.sum -= front;
        }

        self.inner.push_back(item);

        self.sum += item;

        debug_assert!(self.inner.len() <= self.capacity.get());
    }

    pub(crate) fn add_back(&mut self, item: T) {
        if let Some(v) = self.inner.back_mut() {
            *v += item;
            self.sum += item;
        } else {
            self.inner.push_back(item);

            self.sum += item;

            debug_assert!(self.inner.len() <= self.capacity.get());
        }
    }

    #[must_use]
    pub(crate) fn is_full(&self) -> bool {
        self.inner.len() == self.capacity.get()
    }

    #[must_use]
    pub(crate) fn capacity(&self) -> NonZero<usize> {
        self.capacity
    }

    #[must_use]
    pub(crate) fn sum(&self) -> T {
        debug_assert!(self.sum == self.inner.iter().sum());
        self.sum
    }
}
