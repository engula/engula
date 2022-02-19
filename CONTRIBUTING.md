# Contributing to Engula

Thank you for your interest in contributing to Engula! There are many ways to contribute and we appreciate all of them.

If you haven't already, come find us on [the discussion forum](https://github.com/engula/engula/discussions) or [Zulip](https://engula.zulipchat.com).

## Get started

To get started with Engula, follow these steps:

### Set up rust nightly toolchain

We develop Engula with rust nightly toolchain and use [rustup](https://rustup.rs/) to manage toolchain:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
## Choose customize installation and nightly toolchain
```

### Get the source code

Clone the repository:

```sh
git clone https://github.com/engula/engula.git
cd engula
```

### Build and run

Build Engula from the source code:

```sh
cargo build
```

Now that you have the `engula` binary, execute it for exploring:

```sh
cargo run -p engula
```

## Contribute

Engula is developed by an open and friendly community. Everybody is cordially welcome to join the community and contribute to Engula. We value all forms of contributions, including, but not limited to:

* Code reviewing of the existing patches
* Code contribution for bug fixes, improvements, or new features
* Documentation and usage examples
* Community participation in forums and issues
* Test cases to make the codebase more robust
* Tutorials, blog posts, talks that promote the project

Here are guidelines for contributing to various aspect of the project:

* [Principles](#Principles)
* [Report issues](#Report-issues)
* [Review patches](#Review-patches)
* [Contribute code](#Contribute-code)
* [Licenses](#Licenses)

### Principles

Engula community aims to provide harassment-free, welcome and friendly experience for everyone. The first and most important thing for any participant in the community is be friendly and respectful to others. Improper behaviors will be warned and punished. We refuse any kind of harmful behavior to the community or community members. Please read our [Code of Conduct](CODE_OF_CONDUCT.md) and keep proper behavior while participating in the community.

Engula community is a community of peers. All individuals are given the opportunity to participate, but their influence is based on publicly earned merit – what they contribute to the community. Merit lies with the individual, does not expire, is not influenced by employment status or employer, and is non-transferable (merit earned in one project cannot be applied to another).

Engula community requires all communications related to code and decision-making to be publicly accessible to ensure asynchronous collaboration, as necessitated by a globally-distributed community. We adopt GitHub as the single source of truth for all topics, including its [issue tracker](https://github.com/engula/engula/issues), [pull requests](http://github.com/engula/engula/pulls), and [discussion forum](https://github.com/engula/engula/discussions). Besides, we use [Zulip](https://engula.zulipchat.com) chatroom as an auxiliary communication tool.

Private decisions on code, policies, or project direction are disallowed; discourse and transactions outside of Github must be brought back.

### Report issues

If you think you have found an issue in Engula, you can report it to the [issue tracker](https://github.com/engula/engula/issues).

Before filing an issue report is to see whether the problem has already been reported. You can [use the search bar to search existing issues](https://docs.github.com/en/github/administering-a-repository/finding-information-in-a-repository/using-search-to-filter-issues-and-pull-requests). This doesn't always work, and sometimes it's hard to know what to search for, so consider this extra credit. We won't mind if you accidentally file a duplicate report. Don't blame yourself if your issue is closed as duplicated.

If the problem you're reporting is not already in the issue tracker, you can [open a GitHub issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-an-issue) with your GitHub account.

### Review patches

We value any [code review](https://en.wikipedia.org/wiki/Code_review). Reviewing a pull request can be just as informative as providing a pull request and it will allow you to give constructive comments on another developer's work. A code review doesn't have to be perfect. It is helpful to just test the pull request and/or play around with the code and leave comments in the pull request. Do not hesitate to [comment on a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/commenting-on-a-pull-request)!

When you review a pull request, there are several rules and suggestions you should take to write better comments:

* Be respectful to pull request authors and other reviewers. Code review is a part of your community activities. You should follow the community principles.
* Asking questions instead of making statements. The wording of the review comments is very important. To provide review comments that are constructive rather than critical, you can try asking questions rather than making statements.
* Offer sincere praise. Good reviewers focus not only on what is wrong with the code but also on good practices in the code. As a reviewer, you are recommended to offer your encouragement and appreciation to the authors for their good practices in the code.
* Provide additional details and context of your review process. Instead of simply "approving" the pull request. If your test the pull request, report the result and your test environment details. If you request changes, try to suggest how.

### Contribute code

Engula is maintained, improved, and extended by code contributions. We welcome code contributions to Engula. Code contributions use a workflow based on [pull requests](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests).

Contributing to Engula does _not_ start with opening a pull request. We expect contributors to reach out to us first to discuss the overall approach together. Without consensus with the Engula maintainers, contributions might require substantial rework or will not be reviewed. So please create an issue, discuss under an existing issue, or create a topic on the [discussion forum](https://github.com/engula/engula/discussions) and reach consensus.

After a consensus is reached, a maintainer will assign somebody to work on it. Suppose you're the assignee. You can create a patch and [send a pull request to main branch](https://github.com/engula/engula/pull/new/main). When you send a pull request, we're looking forward to an expressive description, clear commit messages, and more test coverage if it is code contribution.

You're supposed to verify your patch locally before submitting the pull request. Engula writes validations with GitHub Actions. You can use [`act`](https://github.com/nektos/act) to run the workflows locally. You can also run the commands directly. Here are some fundamental validations:

* Check style

```sh
cargo clippy --workspace --tests --all-features -- -D warnings
cargo fmt --all -- --check
cargo install taplo-cli
taplo format --check
```

* Run tests

```sh
cargo test --workspace
```

* Check dependency

```sh
cargo install cargo-udeps --locked
cargo udeps --workspace
cargo install cargo-audit
cargo audit
```

### Licenses

The Engula source code is licensed under the [Apache License v2.0](https://www.apache.org/licenses/LICENSE-2.0).

We have a [workflow](https://github.com/engula/engula/actions/workflows/audit-license.yml) to check license headers for most of files, including all code files, scripts, and most of config files, except trivial ones that won't be considered as a work.

The workflow is powered by [Apache SkyWalking Eyes](https://github.com/apache/skywalking-eyes) (`license-eye`). When you find the workflow fails, please add the license header manually or with the instructions of `license-eye`.
