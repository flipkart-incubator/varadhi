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
