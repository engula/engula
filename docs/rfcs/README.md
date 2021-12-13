# Engula RFCs

Many changes, including bug fixes and documentation improvements can be implemented and reviewed via the normal GitHub pull request workflow.

Some changes though are "substantial", and we ask that these be put through a bit of a design process and produce a consensus among the Engula community.

The "RFC" (request for comments) process is intended to provide a consistent and controlled path for new features to enter the project, so that all stakeholders can be confident about the direction the project is evolving in.

## Before creating an RFC

A hastily-proposed RFC can hurt its chances of acceptance. Low quality proposals, proposals for previously-rejected features, or those that don't fit into the near-term roadmap, may be quickly rejected, which can be demotivating for the unprepared contributor. Laying some groundwork ahead of the RFC can make the process smoother.

It is generally a good idea to pursue feedback from other project developers beforehand, to ascertain that the RFC may be desirable; having a consistent impact on the project requires concerted effort toward consensus-building.

The most common preparations for writing and submitting an RFC include talking the idea over on our [official Zulip server](https://engula.zulipchat.com/) or posting "pre-RFCs" on the [discussion forum](https://github.com/engula/engula/discussions).

## What the process is

In short, to get a major feature added to Engula, one must first get the RFC merged under this directory as a markdown file. At that point the RFC is "active" and may be implemented with the goal of eventual inclusion into Engula.

* Fork the this repository.
* Copy `00000000-template.md` to `YYYYMMDD-my-feature.md` (where "my-feature" is descriptive).
* Fill in the RFC and submit a pull request. As a pull request the RFC will receive design feedback from the larger community, and the author should be prepared to revise it in response.
* The maintainers will discuss the RFC pull request in the comment thread of the pull request itself. Offline discussion will be summarized on the pull request comment thread.
* When all stakeholders reach a consensus on the RFC, it will be merged as accepted, or closed as rejected.
