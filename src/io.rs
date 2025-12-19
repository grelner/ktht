use crate::account::{Account, Accounts, Amount, ClientId, TransactionError, TxId};
use crate::rt::Shardable;
use csv::Trim;
use serde::Deserialize;
use std::io::{Read, Write};

/// Represents a transaction type in the csv input format
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CsvTransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// Represents a single transaction in the csv input format
#[derive(Deserialize)]
pub struct CsvTransaction {
    #[serde(rename = "type")]
    tx_type: CsvTransactionType,
    client: ClientId,
    tx: TxId,
    amount: Amount,
}

impl CsvTransaction {
    /// Execute the appropriate method on `Accounts` based on the transaction type.
    pub fn execute_transaction(&self, accounts: &mut Accounts) -> Result<(), TransactionError> {
        match self.tx_type {
            CsvTransactionType::Deposit => accounts.deposit(self.client, self.tx, self.amount),
            CsvTransactionType::Withdrawal => accounts.withdraw(self.client, self.amount),
            CsvTransactionType::Dispute => accounts.dispute(self.client, self.tx),
            CsvTransactionType::Resolve => accounts.resolve(self.client, self.tx),
            CsvTransactionType::Chargeback => accounts.chargeback(self.client, self.tx),
        }
    }
}

/// Allows a transaction to be submitted for processing on a `crate::rt::ShardedThreadPerCoreRuntime`
impl Shardable for CsvTransaction {
    fn shard_id(&self, num_shards: u8) -> usize {
        self.client as usize % num_shards as usize
    }
}

/// A reader for the csv input format.
pub fn csv_transaction_reader<R: Read>(
    reader: R,
) -> csv::DeserializeRecordsIntoIter<R, CsvTransaction> {
    csv::ReaderBuilder::new()
        .trim(Trim::All)
        .from_reader(reader)
        .into_deserialize()
}

/// A writer for the csv output format.
pub struct AccountCsvWriter<W: Write> {
    writer: W,
}

impl<W: Write> AccountCsvWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn write_header(&mut self) -> std::io::Result<()> {
        writeln!(self.writer, "client,available,held,total,locked")
    }

    pub fn write_account(&mut self, client_id: ClientId, account: &Account) -> std::io::Result<()> {
        // ensure our output floats have at most 4 decimal places
        let available = f64::trunc(account.available() * 10000.0) / 10000.0;
        let held = f64::trunc(account.held() * 10000.0) / 10000.0;
        let total = f64::trunc(account.total() * 10000.0) / 10000.0;
        writeln!(
            self.writer,
            "{client_id},{available},{held},{total},{}",
            account.is_locked()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::Accounts;

    #[test]
    fn test_csv_reader() {
        let csv = "type, client, tx, amount\n\
            deposit, 1, 1, 1.0\n\
            deposit, 2, 2, 2.0\n\
            deposit, 1, 3, 2.0\n\
            withdrawal, 1, 4, 1.5\n\
            withdrawal, 2, 5, 3.0";

        let reader = csv_transaction_reader(csv.as_bytes());
        let mut accounts = Accounts::default();
        for tx in reader {
            let tx = tx.unwrap();
            let _ = tx.execute_transaction(&mut accounts);
        }
        assert_eq!(accounts.client_account(1).available(), 1.5);
        assert_eq!(accounts.client_account(2).available(), 2.0);
    }

    #[test]
    fn test_csv_writer() {
        let mut writer = AccountCsvWriter::new(Vec::new());
        writer.write_header().unwrap();
        let mut accounts = Accounts::default();
        accounts.deposit(1, 1, 1.123456).unwrap();
        accounts.deposit(2, 2, 2.123456).unwrap();
        accounts.dispute(2, 2).unwrap();
        let mut accounts = accounts.into_iter().collect::<Vec<_>>();
        accounts.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (client_id, account) in accounts {
            writer.write_account(client_id, &account).unwrap();
        }
        assert_eq!(
            String::from_utf8(writer.writer).unwrap(),
            "client,available,held,total,locked\n\
            1,1.1234,0,1.1234,false\n\
            2,0,2.1234,2.1234,false\n"
        );
    }
}
