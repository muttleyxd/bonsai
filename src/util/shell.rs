//! Shell resolution for command execution.

/// The shell used when `$SHELL` is unset — and by every test.
pub(crate) const POSIX_SHELL: &str = "/bin/sh";

/// The shell that runs user commands: the `bash` tool, background tasks, and
/// eval graders.
///
/// Production hands commands to the operator's own interactive shell, so
/// aliases, functions, and shell-specific syntax behave exactly as they do in
/// their terminal.
///
/// Tests never inherit it. Their commands are POSIX — `i=0`, `$$`, `$!`,
/// `for i in $(seq 1 5000)` — so on a machine whose `$SHELL` is fish six tests
/// failed for reasons unrelated to the code under test (`background::tests::*`
/// and `tool::bash::tests::*`). Tests get [`POSIX_SHELL`] instead, so a green
/// suite means the code is green rather than that the developer's shell
/// happened to accept the syntax.
pub(crate) fn user_shell() -> String {
    if cfg!(test) {
        return POSIX_SHELL.to_string();
    }
    std::env::var("SHELL").unwrap_or_else(|_| POSIX_SHELL.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_shell_is_deterministic_under_test() {
        assert_eq!(user_shell(), POSIX_SHELL);
    }
}
