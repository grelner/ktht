use std::io::stdout;

mod account;
mod io;
mod rt;

fn main() {
    let input_file = std::env::args().nth(1).expect("No input file provided");
    let rt = rt::ShardedThreadPerCoreRuntime::new(num_cpus::get() as u8, process_transaction);
    for tx in io::csv_transaction_reader(std::fs::File::open(input_file).unwrap()) {
        rt.process_item(tx.expect("Malformed csv"))
            .expect("Could not submit transaction");
    }
    let mut csv_writer = io::AccountCsvWriter::new(stdout());
    for (client_id, account) in rt.finish().into_iter().flat_map(|o| o.into_iter()) {
        csv_writer
            .write_account(client_id, &account)
            .expect("Could not write account state to csv");
    }
}

fn process_transaction(accounts: &mut account::Accounts, tx: io::CsvTransaction) {
    // We ignore all errors and continue processing to generate the end state for
    // all accounts no matter what.
    let _ = tx.execute_transaction(accounts);
}
