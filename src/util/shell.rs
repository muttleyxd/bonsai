//! Shell resolution for command execution.

/// The shell used when `$SHELL` is unset — and by every test.
pub(crate) const POSIX_SHELL: &str = "/bin/sh";

/// The shell named by `$SHELL`, or `None` when the variable is unset.
///
/// Split out of [`user_shell`] so the resolver production actually runs is still
/// reachable from a test: `user_shell` itself is pinned under `cfg(test)`.
fn configured_shell() -> Option<String> {
    std::env::var("SHELL").ok()
}

/// The shell that runs user commands: the `bash` tool, background tasks, and
/// eval graders.
///
/// Production hands commands to the operator's own interactive shell, so
/// aliases, functions, and shell-specific syntax behave exactly as they do in
/// their terminal.
#[cfg(not(test))]
pub(crate) fn user_shell() -> String {
    configured_shell().unwrap_or_else(|| POSIX_SHELL.to_string())
}

/// The shell every test runs commands in: [`POSIX_SHELL`], never the
/// developer's own.
///
/// Test commands are POSIX — `i=0`, `$$`, `$!`, `for i in $(seq 1 5000)` — so on
/// a machine whose `$SHELL` is fish six tests failed for reasons unrelated to
/// the code under test (`background::tests::*` and `tool::bash::tests::*`). A
/// green suite then means the code is green rather than that the developer's
/// shell happened to accept the syntax.
#[cfg(test)]
pub(crate) fn user_shell() -> String {
    POSIX_SHELL.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_shell_ignores_the_developers_shell_under_test() {
        crate::util::test_env::with_var("SHELL", Some("/bin/fish"), || {
            assert_eq!(user_shell(), POSIX_SHELL);
        });
    }

    #[test]
    fn configured_shell_reads_the_shell_variable() {
        crate::util::test_env::with_var("SHELL", Some("/bin/fish"), || {
            assert_eq!(configured_shell().as_deref(), Some("/bin/fish"));
        });
        crate::util::test_env::with_var("SHELL", None::<&str>, || {
            assert_eq!(configured_shell(), None);
        });
    }
}
