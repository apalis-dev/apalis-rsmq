# Contributing to apalis-rsmq

Thank you for your interest in contributing to apalis-rsmq! This document provides guidelines and information about contributing to this project.

## Code of Conduct

This project adheres to the Rust Community [Code of Conduct](https://www.rust-lang.org/policies/code-of-conduct). By participating, you are expected to uphold this code.

## Getting Started

### Prerequisites

- Rust 1.75.0 or later
- Redis server for testing
- Git

### Setting Up Development Environment

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/apalis-rsmq.git
   cd apalis-rsmq
   ```

3. Set up the upstream remote:
   ```bash
   git remote add upstream https://github.com/apalis-dev/apalis-rsmq.git
   ```

4. Install development dependencies:
   ```bash
   cargo build
   ```

5. Run the tests to ensure everything is working:
   ```bash
   # Start Redis server first
   export REDIS_URL=redis://localhost:6379
   cargo test
   ```

## Development Workflow

### Before Making Changes

1. Create a new branch for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bugfix-name
   ```

2. Make sure your branch is up to date:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

### Making Changes

1. Write your code following the existing code style
2. Add tests for any new functionality
3. Update documentation if necessary
4. Ensure all tests pass:
   ```bash
   cargo test
   ```

5. Format your code:
   ```bash
   cargo fmt
   ```

6. Run Clippy to catch common mistakes:
   ```bash
   cargo clippy -- -D warnings
   ```

### Commit Guidelines

We follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages:

- `feat: add new feature`
- `fix: bug fix`
- `docs: documentation changes`
- `style: formatting, missing semicolons, etc.`
- `refactor: code refactoring`
- `test: adding tests`
- `chore: maintenance tasks`

Examples:
```
feat: add message deduplication support
fix: handle connection timeouts gracefully
docs: update README with new examples
test: add integration tests for error handling
```

### Submitting Changes

1. Push your branch to your fork:
   ```bash
   git push origin your-branch-name
   ```

2. Create a Pull Request on GitHub with:
   - Clear title describing the change
   - Detailed description of what was changed and why
   - Link to any related issues
   - Screenshots if applicable

## Testing

### Running Tests

```bash
# Unit tests
cargo test --lib

# Integration tests
cargo test --test integration

# All tests
cargo test

# With coverage
cargo tarpaulin --verbose --all-features --workspace --timeout 120
```

### Writing Tests

- Write tests for any new functionality
- Update existing tests when changing behavior
- Use descriptive test names
- Test both success and error cases
- Mock external dependencies when appropriate

## Documentation

### Code Documentation

- Use `///` for public API documentation
- Use `//!` for module-level documentation
- Include examples in documentation when helpful
- Keep documentation up to date with code changes

### README and Guides

- Update README.md if adding new features
- Add examples for new functionality
- Keep installation and usage instructions current

## Performance

- Be mindful of performance implications
- Add benchmarks for performance-critical code
- Profile changes that might affect performance
- Consider memory usage and allocations

## Security

- Be security-conscious in your code
- Don't introduce dependencies with known vulnerabilities
- Follow secure coding practices
- Report security issues privately to the maintainers

## Release Process

Releases are automated through GitHub Actions:

1. Commits to `main` trigger automatic version bumping based on conventional commits
2. Releases are automatically published to crates.io
3. GitHub releases are created with changelog

## Getting Help

- Open an issue for bugs or feature requests
- Join discussions in existing issues
- Ask questions in the discussions section

## License

By contributing to apalis-rsmq, you agree that your contributions will be licensed under the same license as the project (MIT or Apache-2.0).

## Recognition

Contributors will be recognized in the project's README and releases.

Thank you for contributing to apalis-rsmq! 🎉