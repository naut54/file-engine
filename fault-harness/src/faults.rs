/// The v1 fault table from
/// `dev-docs/design/fault-injection-harness.md`. Each variant maps to
/// the `file_engine::Error` it's expected to produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    PermissionDenied,
    DestExists,
    CaseCollision,
    ReservedName,
}

impl Fault {
    /// Faults reachable regardless of what the destination filesystem
    /// is. `PermissionDenied` is planted here even though it's
    /// implemented only on Unix (see `fixtures::build`) — its
    /// non-reachability on Windows is a host-OS gap in this harness, not
    /// a destination-filesystem one, so it doesn't belong in
    /// `DESTINATION_CONDITIONAL` below.
    pub const UNIVERSAL: &'static [Fault] = &[Fault::PermissionDenied, Fault::DestExists];

    /// Faults that only fire against a specific destination filesystem
    /// type — see the "Destination requirement" column in the design
    /// doc. The harness must probe the actual destination
    /// (`dest_probe`) before planting these; `file_engine`'s own
    /// equivalent probe (`profiler::fs_caps`) is `pub(crate)` and not
    /// reachable from here.
    pub const DESTINATION_CONDITIONAL: &'static [Fault] =
        &[Fault::CaseCollision, Fault::ReservedName];

    /// The `file_engine::Error` variant name this fault is expected to
    /// produce, matched against `checker::error_variant_name`.
    pub fn expected_error_name(self) -> &'static str {
        match self {
            Fault::PermissionDenied => "PermissionDenied",
            Fault::DestExists => "DestExists",
            Fault::CaseCollision => "CaseCollision",
            Fault::ReservedName => "ReservedName",
        }
    }
}
