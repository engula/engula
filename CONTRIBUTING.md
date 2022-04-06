# Contributing to Engula

Thank you for your interest in contributing to Engula! There are many ways to contribute, and we appreciate all of them.

## Get started

You can get started with Engula as follows.

Clone the repository:

```sh
git clone https://github.com/engula/engula
cd engula
```

Build from the source code:

```sh
cargo build
```

Run and explore the `engula` binary:

```sh
cargo run -p engula
```

## Contribute

Engula is developed by an open and friendly community. Everybody is welcome to join the community and contribute to Engula. We value all forms of contributions, including, but not limited to:

* Code reviewing of the existing patches
* Code contribution for improvements, bug fixes, or new features
* Test cases to make the codebase more robust
* Documentation and usage examples
* Community participation in issues and discussions
* Tutorials, posts, and talks that promote the project

Here are guidelines for contributing to various aspect of the project:

* [Principles](#Principles)
* [Report issues](#Report-issues)
* [Review patches](#Review-patches)
* [Contribute code](#Contribute-code)
* [Licenses](#Licenses)

### Principles

Engula community aims to provide a harassment-free, welcome and friendly experience for everyone. The first and most important thing for any participant in the community is to be friendly and respectful to others. Improper behaviors will be warned and punished. We refuse any kind of harmful behavior to the community or community members. Please read our [Code of Conduct](CODE_OF_CONDUCT.md) and maintain proper behavior while participating in the community.

Engula community is a community of peers. All individuals are given the opportunity to participate, but their influence is based on publicly earned merit – what they contribute to the community. The merit lies with the individual, does not expire, is not influenced by employment status or employer, and is non-transferable (the merit earned in one project cannot be applied to another).

Engula community requires all communications related to code and decision-making to be publicly accessible to ensure asynchronous collaboration, as necessitated by a globally-distributed community. We adopt GitHub as the single source of truth for all topics, including its [issue tracker](https://github.com/engula/engula/issues), [pull requests](http://github.com/engula/engula/pulls), and [discussions](https://github.com/engula/engula/discussions). Besides, we use [Zulip](https://engula.zulipchat.com) as an auxiliary communication tool.

Private decisions on code, policies, or project direction are disallowed; discourse and transactions outside of Github must be brought back.

### Report issues

If you think you have found an issue in Engula, you can report it to the [issue tracker](https://github.com/engula/engula/issues).

Before filing an issue, please check whether the problem has already been reported. You can [use the search bar to search existing issues](https://docs.github.com/en/github/administering-a-repository/finding-information-in-a-repository/using-search-to-filter-issues-and-pull-requests). This doesn't always work, and sometimes it's hard to know what to search for, so consider this extra credit. We won't mind if you accidentally file a duplicate report. Don't blame yourself if your issue is closed as duplicated.

### Review patches

We value any [code review](https://en.wikipedia.org/wiki/Code_review). Reviewing a pull request can be just as informative as providing a pull request, and it will allow you to give constructive comments on another developer's work. A code review doesn't have to be perfect. It is helpful to just test the pull request and/or play around with the code and leave comments in the pull request. Do not hesitate to [comment on a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/commenting-on-a-pull-request)!

When you review a pull request, there are several rules and suggestions you should take to write better comments:

* Be respectful to pull request authors and other reviewers. Code review is a part of your community activities. You should follow the community principles.
* Asking questions instead of making statements. The wording of the review comments is very important. To provide review comments that are constructive rather than critical, you can try asking questions rather than making statements.
* Offer sincere praise. Good reviewers focus not only on what is wrong with the code but also on good practices in the code. As a reviewer, you are recommended to offer your encouragement and appreciation to the authors for their good practices in the code.
* Provide additional details and context of your review process. Instead of simply "approving" the pull request. If your test the pull request, report the result and your test environment details. If you request changes, try to suggest how.

### Contribute code

Contributing to Engula does _not_ start with opening a pull request. We expect contributors to reach out to us first to discuss the overall approach together. Without consensus with the Engula maintainers, contributions might require substantial rework or will not be reviewed. So please create an issue, follow an existing issue, or create a [discussion](https://github.com/engula/engula/discussions) to reach a consensus first.

After a consensus is reached, a maintainer will assign somebody to work on it. Suppose you're the assignee. You can create a patch and [send a pull request to the main branch](https://github.com/engula/engula/pull/new/main). When you send a pull request, we're looking forward to an expressive description, clear commit messages, and more test coverage if it is a code contribution.

You're supposed to verify your patch locally before submitting the pull request. Engula uses GitHub Actions for validations. You can use [`act`](https://github.com/nektos/act) to run the workflows locally. You can also run the commands directly. Here are some basic validations:

* Run tests

```sh
cargo test --workspace
```

* Check style

```sh
cargo clippy --workspace --tests --all-features -- -D warnings
cargo fmt --all -- --check
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

We have a [workflow](https://github.com/engula/engula/actions/workflows/audit-license.yml) to check license headers for most of the files, including all code files, scripts, and most of the configuration files, except trivial ones that won't be considered as a work.

The workflow is powered by [Apache SkyWalking Eyes](https://github.com/apache/skywalking-eyes) (`license-eye`). When you find the workflow fails, please add the license header manually or with the instructions of `license-eye`.
