use std::env;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShellCd {
    NotCd,
    ChangeTo(PathBuf),
    Invalid(String),
}

pub fn parse(command: &str) -> ShellCd {
    let home = env::var_os("HOME").map(PathBuf::from);
    parse_with_home(command, home.as_deref())
}

pub fn parse_with_home(command: &str, home: Option<&Path>) -> ShellCd {
    let trimmed = command.trim_start();
    if trimmed != "cd" && !trimmed.starts_with("cd ") && !trimmed.starts_with("cd\t") {
        return ShellCd::NotCd;
    }
    if has_unquoted_shell_metachar(trimmed) {
        return ShellCd::NotCd;
    }
    if cd_arg_is_unsupported_special(trimmed) {
        return ShellCd::NotCd;
    }
    let Some(parts) = shlex::split(command) else {
        return ShellCd::Invalid("cd: could not parse quoted path".to_string());
    };
    if parts.first().is_none_or(|part| part != "cd") {
        return ShellCd::NotCd;
    }
    if parts.len() > 2 {
        return ShellCd::NotCd;
    }
    let arg_quoted = cd_arg_is_quoted(trimmed);
    match parts.get(1).map(String::as_str) {
        None => match home {
            Some(home) => ShellCd::ChangeTo(home.to_path_buf()),
            None => ShellCd::Invalid("cd: HOME is not set".to_string()),
        },
        Some(path) if path.starts_with('-') => ShellCd::NotCd,
        Some(path) if path.starts_with('~') && path != "~" && !path.starts_with("~/") => {
            ShellCd::NotCd
        }
        Some(path) if !arg_quoted => ShellCd::ChangeTo(expand_home(path, home)),
        Some(path) => ShellCd::ChangeTo(PathBuf::from(path)),
    }
}

pub fn resolve(target: PathBuf, cwd: &Path) -> Result<PathBuf, String> {
    let path = if target.is_absolute() {
        target
    } else {
        cwd.join(target)
    };
    let resolved = path
        .canonicalize()
        .map_err(|err| format!("cd: failed to resolve {}: {err}", path.display()))?;
    if !resolved.is_dir() {
        return Err(format!("cd: not a directory: {}", resolved.display()));
    }
    Ok(resolved)
}

fn expand_home(path: &str, home: Option<&Path>) -> PathBuf {
    if path == "~" {
        return home.map_or_else(|| PathBuf::from(path), Path::to_path_buf);
    }
    if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = home
    {
        return home.join(rest);
    }
    PathBuf::from(path)
}

fn has_unquoted_shell_metachar(command: &str) -> bool {
    let mut single = false;
    let mut double = false;
    let mut escape = false;
    for ch in command.chars() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if !single => escape = true,
            '\'' if !double => single = !single,
            '"' if !single => double = !double,
            '$' | '`' if !single => return true,
            '&' | '|' | ';' | '<' | '>' | '(' | ')' | '*' | '?' | '[' | ']' | '{' | '}'
            | '\n'
                if !single && !double => {
                return true;
            }
            _ => {}
        }
    }
    false
}

fn cd_arg_is_quoted(command: &str) -> bool {
    command
        .strip_prefix("cd")
        .and_then(|rest| rest.trim_start().chars().next())
        .is_some_and(|ch| ch == '\'' || ch == '"')
}

fn cd_arg_is_unsupported_special(command: &str) -> bool {
    let Some(arg) = command.strip_prefix("cd").map(str::trim_start) else {
        return false;
    };
    arg == "-" || arg.starts_with("\\~")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_non_cd_command() {
        assert_eq!(parse("echo cd"), ShellCd::NotCd);
    }

    #[test]
    fn parse_cd_path_with_spaces() {
        assert_eq!(
            parse("cd 'dir with spaces'"),
            ShellCd::ChangeTo(PathBuf::from("dir with spaces"))
        );
    }

    #[test]
    fn parse_home_uses_supplied_shell_home() {
        let home = Path::new("/shell/home");
        assert_eq!(
            parse_with_home("cd", Some(home)),
            ShellCd::ChangeTo(PathBuf::from("/shell/home"))
        );
        assert_eq!(
            parse_with_home("cd ~/project", Some(home)),
            ShellCd::ChangeTo(PathBuf::from("/shell/home/project"))
        );
    }

    #[test]
    fn leave_complex_cd_for_the_shell() {
        assert_eq!(parse("cd one two"), ShellCd::NotCd);
        assert_eq!(parse("cd one && pwd"), ShellCd::NotCd);
        assert_eq!(parse("cd one&&pwd"), ShellCd::NotCd);
        assert_eq!(parse("cd one;pwd"), ShellCd::NotCd);
        assert_eq!(parse("cd one||pwd"), ShellCd::NotCd);
        assert_eq!(parse("cd $HOME"), ShellCd::NotCd);
        assert_eq!(parse("cd \"$HOME\""), ShellCd::NotCd);
        assert_eq!(parse("cd `pwd`"), ShellCd::NotCd);
        assert_eq!(parse("cd \"$(pwd)\""), ShellCd::NotCd);
        assert_eq!(parse("cd \\~"), ShellCd::NotCd);
        assert_eq!(parse("cd \\~/x"), ShellCd::NotCd);
        assert_eq!(parse("cd -"), ShellCd::NotCd);
        assert_eq!(parse("cd --"), ShellCd::NotCd);
        assert_eq!(parse("cd -P"), ShellCd::NotCd);
        assert_eq!(parse("cd -L"), ShellCd::NotCd);
        assert_eq!(parse("cd build-*"), ShellCd::NotCd);
        assert_eq!(parse("cd maybe?"), ShellCd::NotCd);
        assert_eq!(parse("cd [abc]"), ShellCd::NotCd);
        assert_eq!(parse("cd {a,b}"), ShellCd::NotCd);
        assert_eq!(parse("cd ~otheruser"), ShellCd::NotCd);
        assert_eq!(
            parse("cd 'one;two'"),
            ShellCd::ChangeTo(PathBuf::from("one;two"))
        );
        assert_eq!(parse("cd \"~\""), ShellCd::ChangeTo(PathBuf::from("~")));
    }

    #[test]
    fn resolve_relative_directory_against_cwd() {
        let temp = std::env::temp_dir().join(format!(
            "codex-cd-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&temp);
        std::fs::create_dir(&temp).expect("create temp");
        let child = temp.join("child");
        std::fs::create_dir(&child).expect("create child");

        assert_eq!(
            resolve(PathBuf::from("child"), &temp).expect("resolve"),
            child.canonicalize().expect("canonical child")
        );
        std::fs::remove_dir_all(&temp).expect("remove temp");
    }
}
