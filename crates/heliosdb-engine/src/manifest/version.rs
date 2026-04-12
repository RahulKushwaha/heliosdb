//! VersionSet — in-memory snapshot of which files exist at which levels.

use std::path::{Path, PathBuf};

use super::Edit;

#[derive(Debug, Default)]
pub struct VersionSet {
    /// Current active segment path (there is at most one).
    active_path: Option<PathBuf>,
    /// Inactive segments per level. Index = level number (1-based).
    inactive: Vec<Vec<PathBuf>>,
    /// Next sequence number to assign.
    next_seq: u64,
}

impl VersionSet {
    pub fn apply(&mut self, edit: &Edit) {
        match edit {
            Edit::SetActive { path } => {
                self.active_path = Some(path.clone());
            }
            Edit::AddInactive { level, path } => {
                let level = *level as usize;
                if self.inactive.len() <= level {
                    self.inactive.resize_with(level + 1, Vec::new);
                }
                self.inactive[level].push(path.clone());
            }
            Edit::RemoveInactive { level, path } => {
                let level = *level as usize;
                if let Some(files) = self.inactive.get_mut(level) {
                    files.retain(|p| p != path);
                }
            }
            Edit::SetNextSeq { seq } => {
                self.next_seq = *seq;
            }
        }
    }

    pub fn active_path(&self) -> Option<&Path> {
        self.active_path.as_deref()
    }

    pub fn set_active(&mut self, path: PathBuf) {
        self.active_path = Some(path);
    }

    pub fn inactive_at_level(&self, level: u32) -> &[PathBuf] {
        let level = level as usize;
        self.inactive.get(level).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn all_inactive(&self) -> impl Iterator<Item = (u32, &Path)> {
        self.inactive
            .iter()
            .enumerate()
            .flat_map(|(level, files)| {
                files.iter().map(move |p| (level as u32, p.as_path()))
            })
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn bump_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }
}
