# Choosing an Issue
- For new contributors, start by searching for any open [issues](https://github.com/flipkart-incubator/varadhi/issues) / [backlog](https://github.com/orgs/flipkart-incubator/projects/3/views/5). You can also search for [good-first-issues](https://github.com/flipkart-incubator/varadhi/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).
- Start a discussion on the issue, once you choose to work on it and we will help you in all the ways we can.
- You can also start a [discussion](https://github.com/flipkart-incubator/varadhi/discussions) or a new issue.

# Setting Up your IDE

## IntelliJ IDEA

- Enable annotation processing. Enable `Obtain processors from project classpath`.
- Configure Project JDK to JDK 17, if not configured automatically.
- Code Style will be picked up automatically from the .editorconfig file.

## Development

- Indentation should be 4 spaces. Tabs should never be used.
- Use curly braces even for single-line ifs and elses.
- No @author tags in any javadoc.
- Use try-with-resources blocks whenever is possible.
- TODOs should be associated to at least one issue.
- Always format the contributed code. In Intellij, it is recommended to enable "Reformat Code" & "Optimize Imports"
  via "Tools > Actions on Save".

## Unit tests

- New changes should come with unit tests that verify the functionality being added.
- DO NOT use sleep or other timing assumptions in tests. It is wrong and will fail intermittently on any test server
  with other things going on that causes delays.

# Raising Pull Request
- Fork the repository.
- Make changes for your issue.
- Raise the pull request against flipkart-incubator/varadhi/master branch.
