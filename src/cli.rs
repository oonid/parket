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
