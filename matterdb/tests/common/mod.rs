//! Tests that compare collections and corresponding rust types using proptest.

use matterdb::{Database, Fork, TemporaryDB};
use modifier::Modifier;
use proptest::test_runner::TestCaseResult;

/// Max size of the generated sequence of actions.
pub(crate) const ACTIONS_MAX_LEN: usize = 100;

pub(crate) trait FromFork {
    type Index<'a>;

    fn from_fork(fork: &Fork) -> Self::Index<'_>;
    fn clear(instance: &mut Self::Index<'_>);
}

pub(crate) enum ForkAction {
    Merge,
}

pub(crate) trait AsForkAction {
    fn as_fork_action(&self) -> Option<ForkAction>;
}

pub(crate) fn compare_collections<T, A, R>(
    db: &TemporaryDB,
    actions: &[A],
    compare: impl Fn(&T::Index<'_>, &R) -> TestCaseResult,
) -> TestCaseResult
where
    A: Clone + AsForkAction + Modifier<R> + for<'a> Modifier<T::Index<'a>> + std::fmt::Debug,
    R: Default,
    T: FromFork,
{
    let mut fork = db.fork();
    T::clear(&mut T::from_fork(&fork));
    let mut reference = R::default();

    for action in actions {
        match action.as_fork_action() {
            Some(ForkAction::Merge) => {
                let patch = fork.into_patch();
                db.merge(patch).unwrap();
                fork = db.fork();
            }
            None => {
                let mut collection = T::from_fork(&fork);
                action.clone().modify(&mut collection);
                action.clone().modify(&mut reference);
                compare(&collection, &reference)?;
            }
        }
    }
    let collection = T::from_fork(&fork);
    compare(&collection, &reference)
}
