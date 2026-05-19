//! Stage identification used in errors, stats, and dead-letter routing.

use core::fmt;

/// Identifier for a pipeline stage. Carried by [`crate::Error`] variants
/// and by [`crate::StageFailure`] so a consumer can route or log by
/// stage name.
///
/// Stages assigned through the builder default to descriptive names
/// derived from the operation (`"source"`, `"map"`, `"filter"`,
/// `"batch"`, `"sink"`, ...). Override with
/// [`crate::PipelineBuilder::stage_id`] right before adding the stage.
///
/// # Example
///
/// ```
/// use pipe_io::StageId;
///
/// let id = StageId::new("ingest");
/// assert_eq!(id.as_str(), "ingest");
/// assert_eq!(format!("{id}"), "ingest");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StageId(&'static str);

impl StageId {
    /// Construct a stage identifier from a `&'static str`.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// Return the underlying `&'static str`.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for StageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl From<&'static str> for StageId {
    fn from(value: &'static str) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;

    #[test]
    fn new_and_as_str_roundtrip() {
        let id = StageId::new("ingest");
        assert_eq!(id.as_str(), "ingest");
    }

    #[test]
    fn display_writes_name() {
        let id = StageId::new("ingest");
        assert_eq!(format!("{id}"), "ingest");
    }

    #[test]
    fn from_static_str() {
        let id: StageId = "downstream".into();
        assert_eq!(id.as_str(), "downstream");
    }

    #[test]
    fn equality_by_name() {
        assert_eq!(StageId::new("a"), StageId::new("a"));
        assert_ne!(StageId::new("a"), StageId::new("b"));
    }
}
