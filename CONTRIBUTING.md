# Contributing to Engula

I'm really glad you're reading this, because we need volunteer developers to help this project come to fruition.

If you haven't already, come find us on [the discussion forum][discussion-forum] or [discord][discord-url]. We want you working on things you're excited about.

Welcome to join discussions!

[discussion-forum]: https://github.com/engula/engula/discussions
[discord-url]: https://discord.gg/AN6vgVXaHC

## Get started

We develop Engula with rust nightly toolchain.

You're able to get started with Engula with three steps:

1. Setup the environment with [rustup](https://rustup.rs/).
2. Build Engula via `cargo build`.
3. Run all tests via `cargo test --workspace`.

## Issue reports

If you think you have found an issue in Engula, you can report it to the [issue tracker](https://github.com/engula/engula/issues).

Before filing an issue report is to see whether the problem has already been reported. You can [use the search bar to search existing issues](https://docs.github.com/en/github/administering-a-repository/finding-information-in-a-repository/using-search-to-filter-issues-and-pull-requests). This doesn't always work, and sometimes it's hard to know what to search for, so consider this extra credit. We won't mind if you accidentally file a duplicate report. Don't blame yourself if your issue is closed as duplicated.

If the problem you're reporting is not already in the issue tracker, you can [open a GitHub issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-an-issue) with your GitHub account.

## Pull requests

Please send a [GitHub Pull Request to Engula](https://github.com/engula/engula/pull/new/main) with a clear list of what you've done (read more about [pull requests](http://help.github.com/pull-requests/)). When you send a pull request, we're looking forward to an expressive description, clear commit messages, and more test coverage if it is code contribution.

Before submitting the pull request, please make sure all tests pass locally:

```bash
cargo build
cargo test --workspace
cargo clippy --workspace --tests --all-features -- -D warnings
cargo fmt --all -- --check
```

Thank you for your participation!
