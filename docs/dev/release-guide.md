# Release Guide

## Prerequisites

The release process requires the following privileges:

- [Cargo owner][cargo-owner] of the Engula crates
- Commit permissions to the main and release branch of the Engula project

Engula consists of multiple cargo crates. It is tedious and error-prone to update and publish all crates manually. We can use [cargo-workspaces][cargo-workspaces] to do the job instead.

[cargo-owner]: https://doc.rust-lang.org/cargo/reference/publishing.html#cargo-owner
[cargo-workspaces]: https://github.com/pksunkara/cargo-workspaces

## Branch management

We have two major git branches for now:

- main: all new commits are merged into this branch first
- release: commits for the current release are picked from the main branch to this branch

## Release process

When the current release is almost ready, we can start a release process.

### Prepare the release post (3 days before announce)

Every release comes with a post announcing the release. Writing and reviewing the release post may take more time than expected. So it's a good idea to prepare it a few days before the release.

### Resolve issues and prepare the branches (1 day before announce)

Ensure all issues are resolved, and all commits are landed on the release branch.

### Release day

The following steps assume that you are on the release branch with `origin` pointing to `github.com/engula/engula`.

- **30 minutes before announce** - Publish to crates.io and release on Github
  - In the release branch, bump the version of public crates:

    ```
    cargo workspaces version <bump> --allow-branch release --no-individual-tags
    ```

  - Publish all crates:

    ```
    cargo workspaces publish --from-git
    ```

    Check crates.io to see if everything works.

  - Create a release with the version tag on Github.

  - Close the tracking issues and the milestone for the current release.

- **The announcement**
    - First, merge the release post.
    - Tweet on [Twitter](https://twitter.com/engulaio)
    - Share on [Reddit](https://www.reddit.com/r/rust)
    - Share on [Hacker News](https://news.ycombinator.com/)
    - Send a message to [Zulip](https://engula.zulipchat.com/)

- Take a break to celebrate with all the contributors!

## References

- [Releasing on Github](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases)
- [Publishing on crates.io](https://doc.rust-lang.org/cargo/reference/publishing.html)
- [The Rust Release Process](https://forge.rust-lang.org/release/process.html)
