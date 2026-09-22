use std::collections::BTreeMap;
use std::ops::Bound;

use crate::common::paths::{ancestors_of, child_prefix, is_hidden_path};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileMeta {
    pub chunks: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListEntry {
    pub name: String,
    pub is_directory: bool,
}

#[derive(Debug, Default)]
pub struct Namespace {
    files: BTreeMap<String, FileMeta>,
}

impl Namespace {
    pub fn find(&self, path: &str) -> Option<&FileMeta> {
        self.files.get(path)
    }

    pub fn find_mut(&mut self, path: &str) -> Option<&mut FileMeta> {
        self.files.get_mut(path)
    }

    pub fn exists(&self, path: &str) -> bool {
        self.files.contains_key(path)
    }

    fn descendants<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = (&'a String, &'a FileMeta)> + 'a {
        self.files.range::<str, _>((Bound::Included(prefix), Bound::Unbounded)).take_while(move |(path, _)| path.starts_with(prefix))
    }

    pub fn is_directory(&self, path: &str) -> bool {
        if path == "/" {
            return true;
        }
        let prefix = child_prefix(path);
        self.descendants(&prefix).next().is_some()
    }

    pub fn has_file_ancestor(&self, path: &str) -> bool {
        ancestors_of(path).iter().any(|ancestor| ancestor != "/" && self.exists(ancestor))
    }

    pub fn insert(&mut self, path: &str, meta: FileMeta) -> bool {
        if self.files.contains_key(path) {
            return false;
        }
        self.files.insert(path.to_string(), meta);
        true
    }

    pub fn erase(&mut self, path: &str) -> bool {
        self.files.remove(path).is_some()
    }

    pub fn subtree(&self, root: &str, skip_hidden: bool) -> Vec<(String, FileMeta)> {
        let mut out = Vec::new();
        if let Some(own) = self.find(root)
            && (!skip_hidden || !is_hidden_path(root))
        {
            out.push((root.to_string(), own.clone()));
        }
        let prefix = child_prefix(root);
        for (path, meta) in self.descendants(&prefix) {
            if skip_hidden && is_hidden_path(path) {
                continue;
            }
            out.push((path.clone(), meta.clone()));
        }
        out
    }

    pub fn list(&self, directory: &str, include_hidden: bool) -> Vec<ListEntry> {
        let mut out = Vec::new();
        let prefix = child_prefix(directory);
        let mut last_directory: Option<&str> = None;
        for (path, _) in self.descendants(&prefix) {
            let rest = &path[prefix.len()..];
            match rest.find('/') {
                None => {
                    if !include_hidden && is_hidden_path(path) {
                        continue;
                    }
                    out.push(ListEntry { name: rest.to_string(), is_directory: false });
                }
                Some(slash) => {
                    let name = &rest[..slash];
                    if last_directory == Some(name) {
                        continue;
                    }
                    last_directory = Some(name);
                    out.push(ListEntry { name: name.to_string(), is_directory: true });
                }
            }
        }
        out
    }

    pub fn rename_subtree(&mut self, source: &str, target: &str) {
        let mut moved = Vec::new();
        if let Some(meta) = self.files.remove(source) {
            moved.push((target.to_string(), meta));
        }
        let prefix = child_prefix(source);
        let target_prefix = child_prefix(target);
        let keys: Vec<String> = self.descendants(&prefix).map(|(path, _)| path.clone()).collect();
        for key in keys {
            let meta = self.files.remove(&key).unwrap_or_default();
            moved.push((format!("{}{}", target_prefix, &key[prefix.len()..]), meta));
        }
        for (path, meta) in moved {
            self.files.insert(path, meta);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &FileMeta)> {
        self.files.iter()
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub fn clear(&mut self) {
        self.files.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names_of(entries: &[ListEntry]) -> Vec<String> {
        entries.iter().map(|e| format!("{}{}", e.name, if e.is_directory { "/" } else { "" })).collect()
    }

    fn meta(chunks: &[u64]) -> FileMeta {
        FileMeta { chunks: chunks.to_vec() }
    }

    #[test]
    fn lists_immediate_children_and_implied_directories() {
        let mut ns = Namespace::default();
        ns.insert("/a/b", meta(&[]));
        ns.insert("/a/c/d", meta(&[]));
        ns.insert("/a/c/e", meta(&[]));
        ns.insert("/a/.deleted.5.gone", meta(&[]));
        ns.insert("/z", meta(&[]));
        assert_eq!(names_of(&ns.list("/", false)), vec!["a/", "z"]);
        assert_eq!(names_of(&ns.list("/a", false)), vec!["b", "c/"]);
        assert_eq!(names_of(&ns.list("/a", true)), vec![".deleted.5.gone", "b", "c/"]);
        assert!(ns.list("/nope", false).is_empty());
        assert!(ns.is_directory("/a/c"));
        assert!(ns.is_directory("/"));
        assert!(!ns.is_directory("/a/b"));
        assert!(!ns.is_directory("/a/cc"));
        assert!(ns.has_file_ancestor("/a/b/x"));
        assert!(!ns.has_file_ancestor("/a/c/x"));
    }

    #[test]
    fn subtree_and_rename() {
        let mut ns = Namespace::default();
        ns.insert("/a", meta(&[1]));
        ns.insert("/a/b", meta(&[2]));
        ns.insert("/ab", meta(&[3]));
        let only_file = ns.subtree("/ab", false);
        assert_eq!(only_file.len(), 1);
        assert_eq!(only_file[0].0, "/ab");
        ns.erase("/a");
        ns.insert("/d/x", meta(&[4]));
        ns.insert("/d/y/z", meta(&[5]));
        ns.insert("/d/.deleted.1.h", meta(&[6]));
        let tree = ns.subtree("/d", true);
        assert_eq!(tree.len(), 2);
        assert_eq!(tree[0].0, "/d/x");
        assert_eq!(tree[1].0, "/d/y/z");
        assert_eq!(ns.subtree("/d", false).len(), 3);
        ns.rename_subtree("/d", "/e/f");
        assert!(!ns.is_directory("/d"));
        assert_eq!(ns.find("/e/f/y/z").unwrap().chunks, vec![5]);
        assert!(ns.find("/e/f/.deleted.1.h").is_some());
        ns.rename_subtree("/e/f/x", "/top");
        assert_eq!(ns.find("/top").unwrap().chunks, vec![4]);
        assert!(ns.find("/e/f/x").is_none());
    }
}
