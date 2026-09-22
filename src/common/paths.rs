pub const HIDDEN_PREFIX: &str = ".deleted.";

pub fn is_valid_path(path: &str) -> bool {
    if !path.starts_with('/') {
        return false;
    }
    if path == "/" {
        return true;
    }
    if path.ends_with('/') {
        return false;
    }
    path[1..].split('/').all(|component| !component.is_empty() && component != "." && component != "..")
}

pub fn parent_of(path: &str) -> String {
    if path == "/" {
        return "/".to_string();
    }
    match path.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(slash) => path[..slash].to_string(),
    }
}

pub fn base_name(path: &str) -> &str {
    match path.rfind('/') {
        Some(slash) => &path[slash + 1..],
        None => path,
    }
}

pub fn ancestors_of(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    if path == "/" {
        return out;
    }
    out.push("/".to_string());
    for (i, byte) in path.bytes().enumerate().skip(1) {
        if byte == b'/' {
            out.push(path[..i].to_string());
        }
    }
    out
}

pub fn is_ancestor_or_self(ancestor: &str, path: &str) -> bool {
    if ancestor == path || ancestor == "/" {
        return true;
    }
    path.len() > ancestor.len() && path.starts_with(ancestor) && path.as_bytes()[ancestor.len()] == b'/'
}

pub fn child_prefix(directory: &str) -> String {
    if directory == "/" { "/".to_string() } else { format!("{directory}/") }
}

pub fn is_hidden_path(path: &str) -> bool {
    let name = base_name(path);
    name.len() > HIDDEN_PREFIX.len() && name.starts_with(HIDDEN_PREFIX)
}

pub fn hidden_name_for(path: &str, unix_seconds: i64) -> String {
    format!("{}{}{}.{}", child_prefix(&parent_of(path)), HIDDEN_PREFIX, unix_seconds, base_name(path))
}

pub fn parse_hidden_name(path: &str) -> Option<(i64, String)> {
    if !is_hidden_path(path) {
        return None;
    }
    let rest = &base_name(path)[HIDDEN_PREFIX.len()..];
    let dot = rest.find('.')?;
    if dot == 0 || dot + 1 >= rest.len() {
        return None;
    }
    let stamp = &rest[..dot];
    if !stamp.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let seconds: i64 = stamp.parse().ok()?;
    Some((seconds, format!("{}{}", child_prefix(&parent_of(path)), &rest[dot + 1..])))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_syntax() {
        assert!(is_valid_path("/"));
        assert!(is_valid_path("/a"));
        assert!(is_valid_path("/a/b.c/d"));
        assert!(!is_valid_path(""));
        assert!(!is_valid_path("a"));
        assert!(!is_valid_path("/a/"));
        assert!(!is_valid_path("/a//b"));
        assert!(!is_valid_path("/a/./b"));
        assert!(!is_valid_path("/a/../b"));
    }

    #[test]
    fn parents_and_ancestors() {
        assert_eq!(parent_of("/a/b/c"), "/a/b");
        assert_eq!(parent_of("/a"), "/");
        assert_eq!(parent_of("/"), "/");
        assert_eq!(base_name("/a/b/c"), "c");
        assert_eq!(ancestors_of("/a/b/c"), vec!["/", "/a", "/a/b"]);
        assert!(ancestors_of("/").is_empty());
        assert!(is_ancestor_or_self("/a", "/a/b"));
        assert!(is_ancestor_or_self("/", "/a"));
        assert!(!is_ancestor_or_self("/a", "/ab"));
        assert_eq!(child_prefix("/"), "/");
        assert_eq!(child_prefix("/a"), "/a/");
    }

    #[test]
    fn hidden_names() {
        let hidden = hidden_name_for("/home/user/file", 1725580800);
        assert_eq!(hidden, "/home/user/.deleted.1725580800.file");
        assert!(is_hidden_path(&hidden));
        assert!(!is_hidden_path("/home/user/file"));
        assert_eq!(parse_hidden_name(&hidden), Some((1725580800, "/home/user/file".to_string())));
        assert_eq!(parse_hidden_name("/home/user/.deleted.x.file"), None);
        assert_eq!(hidden_name_for("/top", 7), "/.deleted.7.top");
    }
}
