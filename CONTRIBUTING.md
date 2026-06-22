# Choosing an Issue
- For new contributors, start by searching for any open [issues](https://github.com/flipkart-incubator/varadhi/issues) / [backlog](https://github.com/orgs/flipkart-incubator/projects/3/views/5). You can also search for [good-first-issues](https://github.com/flipkart-incubator/varadhi/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).
- Start a discussion on the issue, once you choose to work on it and we will help you in all the ways we can.
- You can also start a [discussion](https://github.com/flipkart-incubator/varadhi/discussions) or a new issue.

**AI assistants (Cursor, Claude Code):** see [AGENTS.md](./AGENTS.md) for repo map and workflows.

# Setting Up your IDE

## IntelliJ IDEA

- Enable annotation processing. Enable `Obtain processors from project classpath`.
- Configure Project JDK to JDK 25, if not configured automatically.
- Install `Adapter for Eclipse Code Formatter` plugin for code formatting. Configure it via `Settings | Adapter for Eclipse Code Formatter`. Check `Use Eclipse's Code Formatter`. Check `Eclipse workspace/project folder or config file` and choose `varadhi/codestyle.xml`. Choose `VaradhiStyle` as the formatter profile. Check `Optimize Imports`. Under Manual Import Order put `java;;\#java;\#`.

## VSCode

- Install `Java Extension Pack`.
- Configure formatter. `ctrl+shift+p | Java: Open Java Formatter Settings` and choose `varadhi/codestyle.xml` file.


## Development

- Indentation should be 4 spaces. Tabs should never be used.
- Use curly braces even for single-line ifs and elses.
- No @author tags in any javadoc.
- Use try-with-resources blocks whenever is possible.
- TODOs should be associated to at least one issue.
- Always format the contributed code. In Intellij, it is recommended to enable "Reformat Code" & "Optimize Imports"
  via "Tools > Actions on Save".
- For code style enforcement, we are using eclipse jdt formatter. Spotless is already configured in gradlew to validate the code style. `./gradlew spotlessApply` can be used to format the code.

## Unit tests

- New changes should come with unit tests that verify the functionality being added.
- DO NOT use sleep or other timing assumptions in tests. It is wrong and will fail intermittently on any test server
  with other things going on that causes delays.

### Follow these practices while writing UTs:
- Create Mock implementations. 
- Create Useful methods to generate variety of data.
- Use real db, if the test is supposed to validate db interaction.
- Mockito usage should be very minimal and careful. Mocking static method is big NO NO.
- Assert as much as you can. Make least amount of assumptions in the tests.

## Integration tests (E2E)

Server integration tests live under `server/src/testE2E/`. See [README — Integration Tests (E2E)](./README.md#integration-tests-e2e) for the full Docker workflow. Always run `docker compose ... down -v` before starting the test stack so prior runs do not leave stale metadata that breaks setup.

# Raising Pull Request
- Fork the repository.
- Make changes for your issue.
- Raise the pull request against flipkart-incubator/varadhi/master branch.
