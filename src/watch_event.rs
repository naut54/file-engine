use std::path::PathBuf;

/// See dev-docs/design/watch.md. A thin wrapper rather than a re-export of
/// `notify::Event` — insulates the public API from `notify`'s types and
/// version, at the cost of collapsing its more granular sub-kinds
/// (`Create(CreateKind::File/Folder/...)`, etc.) into what most consumers
/// actually branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEventKind {
    Created,
    Modified,
    Removed,
    Other,
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub kind: WatchEventKind,
    pub paths: Vec<PathBuf>,
}

impl From<notify::Event> for WatchEvent {
    fn from(event: notify::Event) -> Self {
        let kind = match event.kind {
            notify::EventKind::Create(_) => WatchEventKind::Created,
            notify::EventKind::Modify(_) => WatchEventKind::Modified,
            notify::EventKind::Remove(_) => WatchEventKind::Removed,
            notify::EventKind::Access(_) | notify::EventKind::Other | notify::EventKind::Any => {
                WatchEventKind::Other
            }
        };
        WatchEvent {
            kind,
            paths: event.paths,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(kind: notify::EventKind) -> notify::Event {
        notify::Event {
            kind,
            paths: vec![PathBuf::from("a")],
            attrs: Default::default(),
        }
    }

    #[test]
    fn create_variants_collapse_to_created() {
        for kind in [
            notify::EventKind::Create(notify::event::CreateKind::File),
            notify::EventKind::Create(notify::event::CreateKind::Folder),
            notify::EventKind::Create(notify::event::CreateKind::Any),
        ] {
            assert_eq!(WatchEvent::from(event(kind)).kind, WatchEventKind::Created);
        }
    }

    #[test]
    fn modify_variants_collapse_to_modified() {
        for kind in [
            notify::EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Any,
            )),
            notify::EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::Any,
            )),
            notify::EventKind::Modify(notify::event::ModifyKind::Any),
        ] {
            assert_eq!(WatchEvent::from(event(kind)).kind, WatchEventKind::Modified);
        }
    }

    #[test]
    fn remove_variants_collapse_to_removed() {
        for kind in [
            notify::EventKind::Remove(notify::event::RemoveKind::File),
            notify::EventKind::Remove(notify::event::RemoveKind::Folder),
            notify::EventKind::Remove(notify::event::RemoveKind::Any),
        ] {
            assert_eq!(WatchEvent::from(event(kind)).kind, WatchEventKind::Removed);
        }
    }

    #[test]
    fn access_other_and_any_collapse_to_other() {
        for kind in [
            notify::EventKind::Access(notify::event::AccessKind::Any),
            notify::EventKind::Other,
            notify::EventKind::Any,
        ] {
            assert_eq!(WatchEvent::from(event(kind)).kind, WatchEventKind::Other);
        }
    }

    #[test]
    fn paths_round_trip_unchanged() {
        let notify_event = event(notify::EventKind::Any);
        let expected = notify_event.paths.clone();
        assert_eq!(WatchEvent::from(notify_event).paths, expected);
    }
}
