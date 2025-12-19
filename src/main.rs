use std::fs::File;
use std::io::stdout;

mod account;
mod io;
mod rt;

/// ```rust
/// The `main` function serves as the entry point of the program. It performs the following steps:
///
/// 1. Reads an input file path from the command line arguments.
/// 2. Opens the input file and initializes a CSV transaction reader to process transaction data.
/// 3. Sets up a multi-threaded runtime (`ShardedThreadPerCoreRuntime`), utilizing a number of threads equal to the number of CPU cores on the system.
/// 4. Processes transactions in parallel by using the `process_transaction` function and aggregates results.
/// 5. Flattens the aggregated results and iterates over each client account.
/// 6. Writes the processed account data to the standard output using an `AccountCsvWriter`.
/// ```
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_file = std::env::args().nth(1).expect("No input file provided");
    let tx_reader = io::csv_transaction_reader(File::open(input_file)?);
    let mut tx_writer = io::AccountCsvWriter::new(stdout());
    rt::ShardedThreadPerCoreRuntime::try_fold(
        // The number of threads used by the system is the number of cores + 1, but since the main
        // thread is mostly IO-bound, this should be ok. In a real system, this would be handled
        // more carefully.
        num_cpus::get() as u8,
        process_transaction,
        tx_reader,
    )?
    .flatten()
    .try_for_each(|(client_id, account)| {
        tx_writer.write_account(client_id, &account)?;
        Ok::<_, csv::Error>(())
    })?;
    Ok(())
}

/// Apply a `io::CsvTransaction` to an `account::Accounts` instance.
fn process_transaction(accounts: &mut account::Accounts, tx: io::CsvTransaction) {
    // We ignore all errors and continue processing to generate the end state for
    // all accounts no matter what.
    let _ = tx.execute_transaction(accounts);
}
