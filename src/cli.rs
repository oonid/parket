use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "parket",
    version,
    about = "Universal MariaDB to Delta Lake Extractor"
)]
pub struct Cli {
    #[arg(
        long,
        help = "Validate configuration and connectivity without extracting data"
    )]
    pub check: bool,

    #[arg(
        long,
        help = "Emit per-batch progress logs with timing and cumulative row counts"
    )]
    pub progress: bool,

    #[arg(
        long,
        help = "Write Delta Lake files to local directory instead of S3 (skips S3 config & connectivity)"
    )]
    pub local: Option<PathBuf>,

    #[arg(
        long,
        value_name = "TABLE",
        help = "Evaluate a single table's columns, indexes, and cursor suitability, then exit (needs only DATABASE_URL)"
    )]
    pub inspect: Option<String>,

    #[arg(
        long,
        value_name = "TABLE",
        num_args = 0..=1,
        help = "Reconcile all synced Delta tables, or one named configured table, against the source DB, then exit"
    )]
    pub verify: Option<Option<String>>,

    #[arg(
        long,
        help = "With --verify, run strict checks even on tables larger than the row cap"
    )]
    pub verify_deep: bool,

    #[arg(
        long,
        help = "After a successful sync, reconcile the synced tables against the source DB; a discrepancy makes the process exit non-zero"
    )]
    pub verify_after: bool,

    // Archive mode: archive the given units of every TABLE_ARCHIVE_ table, then exit.
    #[arg(
        long,
        value_name = "UNIT,...",
        value_delimiter = ',',
        help = "Archive these units (comma-separated ids) in every table of TABLES that has TABLE_ARCHIVE_<table> set, then exit. A unit already archived is skipped"
    )]
    pub archive_units: Option<Vec<i64>>,

    // Archive mode: re-extract the given units and replace exactly their files, then exit.
    #[arg(
        long,
        value_name = "UNIT,...",
        value_delimiter = ',',
        help = "Re-extract these units (comma-separated ids) in every TABLE_ARCHIVE_<table> table and replace exactly their archived files — the repair for a unit that failed verification. Only for units whose source rows have not been deleted; an empty new copy is refused, and so is a copy with fewer rows than the archived one unless --replace-allow-fewer-rows"
    )]
    pub replace_archive_units: Option<Vec<i64>>,

    #[arg(
        long,
        requires = "replace_archive_units",
        help = "With --replace-archive-units, accept a new copy with fewer rows than the archived one (rows deleted from the source on purpose)"
    )]
    pub replace_allow_fewer_rows: bool,

    // Archive mode: verify the given units exactly, source against Delta, then exit.
    #[arg(
        long,
        value_name = "UNIT,...",
        value_delimiter = ',',
        help = "Verify these archived units (comma-separated ids) in every TABLE_ARCHIVE_<table> table: the source scoped to the unit must match Delta exactly. Exit 1 if any unit differs"
    )]
    pub verify_archive_units: Option<Vec<i64>>,

    // §10.7: reclaim superseded/orphaned parquet. Mirrors --verify's shape (all tables, or one).
    #[arg(
        long,
        value_name = "TABLE",
        num_args = 0..=1,
        help = "Reclaim superseded/orphaned parquet for all configured tables, or one named table, then exit. DRY RUN unless --vacuum-apply"
    )]
    pub vacuum: Option<Option<String>>,

    #[arg(
        long,
        help = "With --vacuum, actually delete. Without it --vacuum only reports what WOULD be removed"
    )]
    pub vacuum_apply: bool,

    #[arg(
        long,
        value_name = "HOURS",
        default_value_t = 168,
        help = "With --vacuum, retention window in hours (default 168 = 7 days, the PS-M3 cadence)"
    )]
    pub vacuum_retention_hours: u64,

    #[arg(
        long,
        help = "With --vacuum, use VacuumMode::Full so parquet that no commit ever referenced (orphans from aborted runs) is reclaimed too; delta-rs defaults to Lite, which skips those"
    )]
    pub vacuum_full: bool,

    #[arg(
        long,
        help = "With --vacuum, permit --vacuum-retention-hours below 168; required because a short retention can delete files in-flight readers still need"
    )]
    pub vacuum_force: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn default_mode_is_run() {
        let cli = Cli::try_parse_from(["parket"]).unwrap();
        assert!(!cli.check);
        assert!(cli.verify.is_none());
    }

    #[test]
    fn check_flag_parsed() {
        let cli = Cli::try_parse_from(["parket", "--check"]).unwrap();
        assert!(cli.check);
    }

    #[test]
    fn version_flag_recognized() {
        let result = Cli::try_parse_from(["parket", "--version"]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            format!("{err}").contains("parket"),
            "version output should contain program name"
        );
    }

    #[test]
    fn help_flag_recognized() {
        let result = Cli::try_parse_from(["parket", "--help"]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        let output = format!("{err}");
        assert!(
            output.contains("Universal MariaDB to Delta Lake Extractor"),
            "help should contain about text"
        );
        assert!(
            output.contains("--check"),
            "help should mention --check flag"
        );
        assert!(
            output.contains("--progress"),
            "help should mention --progress flag"
        );
    }

    #[test]
    fn progress_flag_parsed() {
        let cli = Cli::try_parse_from(["parket", "--progress"]).unwrap();
        assert!(cli.progress);
        assert!(!cli.check);
        assert!(cli.verify.is_none());
    }

    #[test]
    fn check_and_progress_together() {
        let cli = Cli::try_parse_from(["parket", "--check", "--progress"]).unwrap();
        assert!(cli.check);
        assert!(cli.progress);
    }

    #[test]
    fn default_no_flags() {
        let cli = Cli::try_parse_from(["parket"]).unwrap();
        assert!(!cli.check);
        assert!(!cli.progress);
        assert!(cli.verify.is_none());
    }

    #[test]
    fn local_flag_with_dir() {
        let cli = Cli::try_parse_from(["parket", "--local", "/tmp/delta"]).unwrap();
        assert!(!cli.check);
        assert!(!cli.progress);
        assert_eq!(
            cli.local.as_deref(),
            Some(std::path::Path::new("/tmp/delta"))
        );
    }

    #[test]
    fn local_flag_with_relative_dir() {
        let cli = Cli::try_parse_from(["parket", "--local", "output/delta"]).unwrap();
        assert!(cli.local.is_some());
        assert!(cli.local.as_deref().unwrap().ends_with("output/delta"));
    }

    #[test]
    fn local_flag_without_arg_fails() {
        let result = Cli::try_parse_from(["parket", "--local"]);
        assert!(result.is_err());
    }

    #[test]
    fn local_with_check_and_progress() {
        let cli =
            Cli::try_parse_from(["parket", "--local", "/data", "--check", "--progress"]).unwrap();
        assert!(cli.check);
        assert!(cli.progress);
        assert_eq!(cli.local.as_deref(), Some(std::path::Path::new("/data")));
    }

    #[test]
    fn local_flag_none_by_default() {
        let cli = Cli::try_parse_from(["parket"]).unwrap();
        assert!(cli.local.is_none());
    }

    #[test]
    fn help_mentions_local_flag() {
        let result = Cli::try_parse_from(["parket", "--help"]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        let output = format!("{err}");
        assert!(
            output.contains("--local"),
            "help should mention --local flag"
        );
    }

    #[test]
    fn inspect_flag_with_table_name() {
        let cli = Cli::try_parse_from(["parket", "--inspect", "orders"]).unwrap();
        assert_eq!(cli.inspect.as_deref(), Some("orders"));
    }

    #[test]
    fn inspect_flag_absent() {
        let cli = Cli::try_parse_from(["parket"]).unwrap();
        assert!(cli.inspect.is_none());
    }

    #[test]
    fn help_mentions_inspect_flag() {
        let result = Cli::try_parse_from(["parket", "--help"]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        let output = format!("{err}");
        assert!(
            output.contains("--inspect"),
            "help should mention --inspect flag"
        );
    }

    #[test]
    fn archive_units_flag_parses_a_comma_separated_list() {
        let cli = Cli::try_parse_from(["parket", "--archive-units", "909,856"]).unwrap();
        assert_eq!(cli.archive_units, Some(vec![909, 856]));
        assert!(Cli::try_parse_from(["parket", "--archive-units", "909,abc"]).is_err());
        assert!(Cli::try_parse_from(["parket"]).unwrap().archive_units.is_none());
        let cli = Cli::try_parse_from(["parket", "--verify-archive-units", "909"]).unwrap();
        assert_eq!(cli.verify_archive_units, Some(vec![909]));
    }

    #[test]
    fn allowing_a_smaller_copy_needs_a_replace() {
        let cli = Cli::try_parse_from(["parket", "--replace-archive-units", "909"]).unwrap();
        assert_eq!(cli.replace_archive_units, Some(vec![909]));
        assert!(!cli.replace_allow_fewer_rows, "refused by default");
        let cli = Cli::try_parse_from(["parket", "--replace-archive-units", "909", "--replace-allow-fewer-rows"]).unwrap();
        assert!(cli.replace_allow_fewer_rows);
        assert!(Cli::try_parse_from(["parket", "--replace-allow-fewer-rows"]).is_err());
    }

    #[test]
    fn verify_flag_without_table_parsed() {
        let cli = Cli::try_parse_from(["parket", "--verify"]).unwrap();
        assert_eq!(cli.verify, Some(None));
    }

    #[test]
    fn verify_flag_with_table_parsed() {
        let cli = Cli::try_parse_from(["parket", "--verify", "orders"]).unwrap();
        assert_eq!(cli.verify, Some(Some("orders".to_string())));
    }

    #[test]
    fn verify_flag_with_table_and_deep_parsed() {
        let cli = Cli::try_parse_from(["parket", "--verify", "orders", "--verify-deep"])
            .unwrap();
        assert_eq!(cli.verify, Some(Some("orders".to_string())));
        assert!(cli.verify_deep);
    }

    #[test]
    fn bare_table_argument_is_rejected() {
        let result = Cli::try_parse_from(["parket", "orders"]);
        assert!(result.is_err());
    }
}
