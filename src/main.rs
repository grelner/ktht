use std::fs::File;
use std::io::stdout;

mod account;
mod io;
mod rt;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_file = std::env::args().nth(1).expect("No input file provided");
    let tx_reader = io::csv_transaction_reader(File::open(input_file)?);
    let mut tx_writer = io::AccountCsvWriter::new(stdout());
    rt::ShardedThreadPerCoreRuntime::try_fold(
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

fn process_transaction(accounts: &mut account::Accounts, tx: io::CsvTransaction) {
    // We ignore all errors and continue processing to generate the end state for
    // all accounts no matter what.
    let _ = tx.execute_transaction(accounts);
}
