use crate::account::{Account, Accounts, Amount, ClientId, TransactionError, TxId};
use crate::rt::Shardable;
use csv::Trim;
use serde::{Deserialize, Serialize};
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

/// The csv output format for account state
#[derive(Serialize)]
pub struct AccountStateCsv {
    client: ClientId,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

impl AccountStateCsv {
    pub fn from_account(client_id: ClientId, account: &Account) -> Self {
        Self {
            client: client_id,
            available: account.available(),
            held: account.held(),
            total: account.total(),
            locked: account.is_locked(),
        }
    }
}

/// A writer for the csv output format.
pub struct AccountCsvWriter<W: Write> {
    writer: csv::Writer<W>,
}

impl<W: Write> AccountCsvWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: csv::Writer::from_writer(writer),
        }
    }
    pub fn write_account(&mut self, client_id: ClientId, account: &Account) -> csv::Result<()> {
        self.writer
            .serialize(AccountStateCsv::from_account(client_id, account))
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
        writer.write_account(1, &Account::default()).unwrap();
        writer.write_account(2, &Account::default()).unwrap();
        assert_eq!(
            String::from_utf8(writer.writer.into_inner().unwrap()).unwrap(),
            "client,available,held,total,locked\n1,0.0,0.0,0.0,false\n2,0.0,0.0,0.0,false\n"
        );
    }
}
