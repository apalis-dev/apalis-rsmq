# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- Comprehensive CI/CD pipelines with GitHub Actions
- Automated testing, linting, and security audits
- Automated dependency updates and vulnerability scanning
- Code coverage reporting with codecov
- Automated releases with semantic versioning
- Performance benchmarking
- Documentation generation and deployment
- Contribution guidelines and development workflow

### Changed
- Enhanced README with CI badges and comprehensive documentation
- Added development tooling configuration (rustfmt, clippy, deny)

## [0.1.0-alpha.1] - Initial Release

### Added
- Initial implementation of Redis-backed message queue
- Integration with apalis framework
- Message enqueue and dequeue functionality
- Acknowledgment system for processed messages
- Configurable polling intervals
- Type-safe message handling with Serde
- Basic examples and documentation