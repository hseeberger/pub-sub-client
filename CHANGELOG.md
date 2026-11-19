# Changelog


## [0.13.6](https://github.com/hseeberger/pub-sub-client/compare/v0.13.5...v0.13.6) - 2026-11-02

### Bug Fixes

- Update rustls-webpki and webpki roots for RUSTSEC ([#154](https://github.com/hseeberger/pub-sub-client/pull/154))
- Bump rustls to 0.23.46 ([#143](https://github.com/hseeberger/pub-sub-client/pull/143))
- Bump quinn-proto to 0.11.19 ([#138](https://github.com/hseeberger/pub-sub-client/pull/138))

### Miscellaneous Tasks

- Rename docker-compose.yaml to compose.yaml ([#165](https://github.com/hseeberger/pub-sub-client/pull/165))

## [0.13.5](https://github.com/hseeberger/pub-sub-client/compare/v0.13.4...v0.13.5) - 2026-09-19

### Bug Fixes

- Bump rustls to 0.23.45 for RUSTSEC-2026-0285 ([#116](https://github.com/hseeberger/pub-sub-client/pull/116))

### Miscellaneous Tasks

- Unify docker-compose and read its facts via composed ([#99](https://github.com/hseeberger/pub-sub-client/pull/99))

## [0.13.4](https://github.com/hseeberger/pub-sub-client/compare/v0.13.3...v0.13.4) - 2026-08-20

### Refactor

- Unify tracing initialization ([#89](https://github.com/hseeberger/pub-sub-client/pull/89))

### Documentation

- Turn on warnings for missing docs ([#67](https://github.com/hseeberger/pub-sub-client/pull/67))

## [0.13.3](https://github.com/hseeberger/pub-sub-client/compare/v0.13.2...v0.13.3) - 2026-07-16

### Miscellaneous Tasks

- Standardize project scaffolding and package metadata ([#48](https://github.com/hseeberger/pub-sub-client/pull/48))

## [0.13.2](https://github.com/hseeberger/pub-sub-client/compare/v0.13.1...v0.13.2) - 2026-06-30

### Documentation

- Tighten README ([#41](https://github.com/hseeberger/pub-sub-client/pull/41))

## [0.13.1](https://github.com/hseeberger/pub-sub-client/compare/v0.13.0...v0.13.1) - 2026-06-13

### Features

- Add topic and subscription management ([#35](https://github.com/hseeberger/pub-sub-client/pull/35))

## [0.13.0](https://github.com/hseeberger/pub-sub-client/compare/v0.12.0...v0.13.0) - 2026-06-13

### Bug Fixes

- Change-compromised-key ([#20](https://github.com/hseeberger/pub-sub-client/pull/20))

### Documentation

- Improve README

### Miscellaneous Tasks

- Big cleanup and update ([#26](https://github.com/hseeberger/pub-sub-client/pull/26))
- Cleanup logging related code ([#19](https://github.com/hseeberger/pub-sub-client/pull/19))

## [0.12.0](https://github.com/hseeberger/pub-sub-client/compare/pub-sub-client-v0.11.0...v0.12.0) - 2023-10-12

### Features

- Get rid of PublishedMessage derive ([#18](https://github.com/hseeberger/pub-sub-client/pull/18))

### Miscellaneous Tasks

- Updates ([#17](https://github.com/hseeberger/pub-sub-client/pull/17))
- Badges, update GH workflows, dependabot ([#16](https://github.com/hseeberger/pub-sub-client/pull/16))

## [0.11.0](https://github.com/hseeberger/pub-sub-client/compare/pub-sub-client-v0.10.0...pub-sub-client-v0.11.0) - 2022-10-11

### Bug Fixes

- Compilation error

### Miscellaneous Tasks

- Major cleanup and improved repo security ([#15](https://github.com/hseeberger/pub-sub-client/pull/15))
- Simplify some code ([#14](https://github.com/hseeberger/pub-sub-client/pull/14))

## [0.10.0](https://github.com/hseeberger/pub-sub-client/compare/pub-sub-client-v0.9.2...pub-sub-client-v0.10.0) - 2022-02-24

### Features

- Change subscription API ([#13](https://github.com/hseeberger/pub-sub-client/pull/13))

## [0.9.2](https://github.com/hseeberger/pub-sub-client/compare/pub-sub-client-v0.9.1...pub-sub-client-v0.9.2) - 2022-02-10

### Bug Fixes

- Prevent unexpected_http_status_code from panicking ([#12](https://github.com/hseeberger/pub-sub-client/pull/12))

### Documentation

- Minor README changes

### Miscellaneous Tasks

- Simplify code with anonymous lifetimes ([#10](https://github.com/hseeberger/pub-sub-client/pull/10))

## [0.9.1](https://github.com/hseeberger/pub-sub-client/compare/pub-sub-client-v0.9.0...pub-sub-client-v0.9.1) - 2022-01-29

### Bug Fixes

- Add missing context bounds to Error::Initialization ([#9](https://github.com/hseeberger/pub-sub-client/pull/9))

## [0.9.0](https://github.com/hseeberger/pub-sub-client/compare/v0.8.0...pub-sub-client-v0.9.0) - 2022-01-29

### Features

- Publish with attributes and ordering_key ([#8](https://github.com/hseeberger/pub-sub-client/pull/8))

### Documentation

- Update README

## [0.8.0](https://github.com/hseeberger/pub-sub-client/compare/v0.7.0...v0.8.0) - 2022-01-24

### Features

- Add publisher ([#4](https://github.com/hseeberger/pub-sub-client/pull/4))

### Bug Fixes

- Set version back to correct value 0.7.0

### Documentation

- Align README with simple example

### Miscellaneous Tasks

- Change dependency format for 0.*, improve examples ([#6](https://github.com/hseeberger/pub-sub-client/pull/6))

## [0.7.0](https://github.com/hseeberger/pub-sub-client/compare/v0.6.1...v0.7.0) - 2022-01-21

### Refactor

- Use new (1.58) format strings with catpured args ([#3](https://github.com/hseeberger/pub-sub-client/pull/3))
- Modularize lib.rs ([#2](https://github.com/hseeberger/pub-sub-client/pull/2))

## [0.6.1](https://github.com/hseeberger/pub-sub-client/compare/v0.6.0...v0.6.1) - 2022-01-11

### Documentation

- Add simple example and update README

## [0.6.0](https://github.com/hseeberger/pub-sub-client/compare/v0.5.0...v0.6.0) - 2022-01-10

### Features

- Refine trait bound for boxed std Error

## [0.5.0](https://github.com/hseeberger/pub-sub-client/compare/v0.4.1...v0.5.0) - 2022-01-10

### Features

- Use boxed std Error for transform

## [0.4.0](https://github.com/hseeberger/pub-sub-client/compare/v0.3.1...v0.4.0) - 2022-01-02

### Features

- Add optional timeout param to pull and acknowledge

## [0.3.0](https://github.com/hseeberger/pub-sub-client/compare/v0.2.0...v0.3.0) - 2021-12-31

### Features

- Add pull_raw, remove pull_insert_attribute

### Miscellaneous Tasks

- Bump version

## [0.2.0](https://github.com/hseeberger/pub-sub-client/compare/v0.1.0...v0.2.0) - 2021-12-29

### Features

- Switch to subscription ID

## [0.1.0](https://github.com/hseeberger/pub-sub-client/releases/tag/v0.1.0) - 2021-12-29

### Features

- Acknowledge
- Finish pull with tests
- First cut

### Refactor

- Address clippy hints
- Get rid of transform module

### Documentation

- Fix README

### Miscellaneous Tasks

- Little things

### Other

- Add simple example
- Use secret service account key
- Registered in blackbox: secrets/cryptic-hawk-336616-e228f9680cbc.json
- Add BlackBox admin 7E5AC6C025D5361F
- Initialize BlackBox
