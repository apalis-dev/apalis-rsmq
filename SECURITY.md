# Security Policy

## Supported Versions

We actively support the following versions of apalis-rsmq:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security vulnerability in apalis-rsmq, please report it privately.

### How to Report

**Please do NOT create a public GitHub issue for security vulnerabilities.**

Instead, please send an email to: [security@apalis.dev](mailto:security@apalis.dev)

Include the following information:

- A clear description of the vulnerability
- Steps to reproduce the issue
- Potential impact of the vulnerability
- Any suggested fixes or mitigations

### Response Timeline

- **Acknowledgment**: We will acknowledge receipt of your report within 2 business days
- **Assessment**: We will assess the vulnerability and provide an initial response within 5 business days
- **Resolution**: We aim to resolve critical vulnerabilities within 30 days

### Security Measures

Our project includes several automated security measures:

- **Dependency Auditing**: Automated scanning for known vulnerabilities in dependencies
- **Static Analysis**: Code analysis for potential security issues
- **Regular Updates**: Automated dependency updates to address security patches

### Responsible Disclosure

We believe in responsible disclosure. We ask that you:

- Give us reasonable time to investigate and fix the issue
- Do not publicly disclose the vulnerability until we have released a fix
- Do not exploit the vulnerability for malicious purposes

### Recognition

We appreciate security researchers who help make apalis-rsmq safer. With your permission, we will acknowledge your contribution in:

- Security advisories
- Release notes
- Project contributors list

## Security Best Practices

When using apalis-rsmq:

- Keep your dependencies up to date
- Use secure Redis configurations (authentication, TLS, firewall rules)
- Validate and sanitize message content
- Monitor for unusual activity
- Follow the principle of least privilege for Redis access

Thank you for helping keep apalis-rsmq secure!