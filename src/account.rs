use fnv::FnvHashMap;
use std::collections::hash_map;

pub type TxId = u32;
pub type ClientId = u16;

// Since we know that the float precision is limited to 4, we could potentially find some speed by
// representing these internally as integers, depending on the workload. They are kept as floats here
// for code clarity.
pub type Amount = f32;
#[derive(Debug)]
pub enum TransactionError {
    AccountLocked,
    InsufficientFunds,
    NotDisputed,
    AlreadyDisputed,
    TransactionNotFound,
    DuplicateTransaction,
}

struct Deposit {
    amount: Amount,
    disputed: bool,
}

/// Represents the account of a single client
#[derive(Default)]
pub struct Account {
    // Our keys are just 4 bytes, so let's use Fnv hashing to speed things up
    deposits: FnvHashMap<TxId, Deposit>,
    total: f64, // While transaction amounts are f32, let's make these f64 just in case someone deposits a lot
    held: f64,
    locked: bool,
}

impl Account {
    /// A deposit is a credit to the client's asset account, meaning it should increase the available and
    /// total funds of the client account
    ///
    /// # Errors
    /// - `DuplicateTransaction` if a deposit with this id has already been processed
    /// - `AccountLocked` if the account is locked
    pub fn deposit(&mut self, tx_id: TxId, amount: Amount) -> Result<(), TransactionError> {
        self.check_not_locked()?;
        match self.deposits.entry(tx_id) {
            hash_map::Entry::Occupied(_) => Err(TransactionError::DuplicateTransaction),
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Deposit {
                    amount,
                    disputed: false,
                });
                self.total += amount as f64;
                Ok(())
            }
        }
    }

    /// A withdraw is a debit to the client's asset account, meaning it should decrease the available and
    /// total funds of the client account
    ///
    /// # Errors
    /// - `InsufficientFunds` if the withdrawal puts the account into overdraft
    /// - `AccountLocked` if the account is locked
    pub fn withdraw(&mut self, amount: Amount) -> Result<(), TransactionError> {
        self.check_not_locked()?;
        let amount = amount as f64;
        if self.available() < amount {
            Err(TransactionError::InsufficientFunds)
        } else {
            self.total -= amount;
            Ok(())
        }
    }

    /// A dispute represents a client's claim that a transaction was erroneous and should be reversed.
    ///
    /// The transaction shouldn't be reversed yet but the associated funds should be held. This means
    /// that the clients' available funds should decrease by the amount disputed, their held funds should
    /// increase by the amount disputed, while their total funds should remain the same.
    ///
    /// # Errors
    /// - `AlreadyDisputed` if the transaction is already in the disputed state.
    /// - `TransactionNotFound` if the transaction does not exist.
    /// - `AccountLocked` if the account is locked.
    pub fn dispute(&mut self, tx_id: TxId) -> Result<(), TransactionError> {
        self.check_not_locked()?;
        if let Some(disputed_deposit) = self.deposits.get_mut(&tx_id) {
            if disputed_deposit.disputed {
                Err(TransactionError::AlreadyDisputed)
            } else {
                self.held += disputed_deposit.amount as f64;
                disputed_deposit.disputed = true;
                Ok(())
            }
        } else {
            Err(TransactionError::TransactionNotFound)
        }
    }

    /// A resolve represents a resolution to a dispute, releasing the associated held funds. Funds that
    /// were previously disputed are no longer disputed. This means that the clients held funds should
    /// decrease by the amount no longer disputed, their available funds should increase by the amount
    /// no longer disputed, and their total funds should remain the same.
    ///
    /// # Errors
    /// - `NotDisputed` if the transaction is not disputed
    /// - `TransactionNotFound` if the transaction does not exist
    /// - `AccountLocked` if the account is locked
    pub fn resolve(&mut self, tx_id: TxId) -> Result<(), TransactionError> {
        self.check_not_locked()?;
        if let Some(disputed_deposit) = self.deposits.get_mut(&tx_id) {
            if disputed_deposit.disputed {
                self.held -= disputed_deposit.amount as f64;
                disputed_deposit.disputed = false;
                Ok(())
            } else {
                Err(TransactionError::NotDisputed)
            }
        } else {
            Err(TransactionError::TransactionNotFound)
        }
    }

    /// A chargeback is the final state of a dispute and represents the client reversing a transaction.
    /// Funds that were held have now been withdrawn. This means that the clients held funds and total
    /// funds should decrease by the amount previously disputed. If a chargeback occurs the client's
    /// account should be immediately frozen.
    ///
    /// # Errors
    /// - `NotDisputed` if the transaction is not disputed
    /// - `TransactionNotFound` if the transaction does not exist
    /// - `AccountLocked` if the account is locked
    pub fn chargeback(&mut self, tx_id: TxId) -> Result<(), TransactionError> {
        self.check_not_locked()?;
        if let Some(disputed_deposit) = self.deposits.get(&tx_id) {
            if disputed_deposit.disputed {
                let disputed_amount = disputed_deposit.amount as f64;
                self.held -= disputed_amount;
                self.total -= disputed_amount;
                self.locked = true;
                Ok(())
            } else {
                Err(TransactionError::NotDisputed)
            }
        } else {
            Err(TransactionError::TransactionNotFound)
        }
    }

    /// Return `Err(TransactionError::AccountLocked)` if the account is locked. Otherwise, Ok(()).
    #[inline]
    fn check_not_locked(&self) -> Result<(), TransactionError> {
        if self.locked {
            Err(TransactionError::AccountLocked)
        } else {
            Ok(())
        }
    }

    /// Available funds
    pub fn available(&self) -> f64 {
        self.total - self.held
    }

    /// Total funds, e.g., available plus held
    pub fn total(&self) -> f64 {
        self.total
    }

    /// Held funds, e.g., funds that are disputed
    pub fn held(&self) -> f64 {
        self.held
    }

    /// Indicates whether the account is locked.
    pub fn is_locked(&self) -> bool {
        self.locked
    }
}

/// A collection of accounts, indexed by client id
#[derive(Default)]
pub struct Accounts {
    accounts: FnvHashMap<ClientId, Account>,
}

impl Accounts {
    pub fn client_account(&mut self, client_id: ClientId) -> &mut Account {
        self.accounts.entry(client_id).or_default()
    }

    pub fn deposit(
        &mut self,
        client_id: ClientId,
        tx_id: TxId,
        amount: Amount,
    ) -> Result<(), TransactionError> {
        self.client_account(client_id).deposit(tx_id, amount)
    }

    pub fn withdraw(
        &mut self,
        client_id: ClientId,
        amount: Amount,
    ) -> Result<(), TransactionError> {
        self.client_account(client_id).withdraw(amount)
    }

    pub fn dispute(&mut self, client_id: ClientId, tx_id: TxId) -> Result<(), TransactionError> {
        self.client_account(client_id).dispute(tx_id)
    }

    pub fn resolve(&mut self, client_id: ClientId, tx_id: TxId) -> Result<(), TransactionError> {
        self.client_account(client_id).resolve(tx_id)
    }

    pub fn chargeback(&mut self, client_id: ClientId, tx_id: TxId) -> Result<(), TransactionError> {
        self.client_account(client_id).chargeback(tx_id)
    }
}

impl IntoIterator for Accounts {
    type Item = (ClientId, Account);
    type IntoIter = hash_map::IntoIter<ClientId, Account>;

    fn into_iter(self) -> Self::IntoIter {
        self.accounts.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_balances(account: &Account, available: f64, held: f64, total: f64) {
        assert_eq!(account.available(), available);
        assert_eq!(account.held(), held);
        assert_eq!(account.total(), total);
    }
    #[test]
    fn test_dispute_resolve() {
        let mut account = Account::default();
        assert!(account.deposit(1, 100.0).is_ok());
        assert!(account.deposit(2, 100.0).is_ok());
        assert_balances(&account, 200.0, 0.0, 200.0);
        assert!(account.dispute(1).is_ok());
        assert_balances(&account, 100.0, 100.0, 200.0);
        assert!(account.resolve(1).is_ok());
        assert_balances(&account, 200.0, 0.0, 200.0);
    }

    #[test]
    fn test_dispute_chargeback() {
        let mut account = Account::default();
        assert!(account.deposit(1, 100.0).is_ok());
        assert!(account.deposit(2, 100.0).is_ok());
        assert!(account.dispute(1).is_ok());
        assert!(account.chargeback(1).is_ok());
        assert_balances(&account, 100.0, 0.0, 100.0);
        assert!(account.is_locked())
    }

    #[test]
    fn test_double_dispute() {
        let mut account = Account::default();
        assert!(account.deposit(1, 100.0).is_ok());
        assert_balances(&account, 100.0, 0.0, 100.0);
        assert!(account.dispute(1).is_ok());
        assert!(matches!(
            account.dispute(1),
            Err(TransactionError::AlreadyDisputed)
        ));
    }

    #[test]
    fn test_resolve_non_dispute() {
        let mut account = Account::default();
        assert!(account.deposit(1, 100.0).is_ok());
        assert!(matches!(
            account.resolve(1),
            Err(TransactionError::NotDisputed)
        ));
    }

    #[test]
    fn test_chargeback_non_dispute() {
        let mut account = Account::default();
        assert!(account.deposit(1, 100.0).is_ok());
        assert!(matches!(
            account.chargeback(1),
            Err(TransactionError::NotDisputed)
        ));
    }

    #[test]
    fn test_insufficient_funds() {
        let mut account = Account::default();
        assert!(matches!(
            account.withdraw(100.0),
            Err(TransactionError::InsufficientFunds)
        ));
    }

    #[test]
    fn test_duplicate_transaction() {
        let mut account = Account::default();
        assert!(account.deposit(1, 100.0).is_ok());
        assert!(matches!(
            account.deposit(1, 200.0),
            Err(TransactionError::DuplicateTransaction)
        ));
        assert_balances(&account, 100.0, 0.0, 100.0);
    }

    #[test]
    fn test_deposit_withdraw() {
        let mut account = Account::default();
        assert!(account.deposit(1, 100.0).is_ok());
        assert!(account.withdraw(99.0).is_ok());
        assert_balances(&account, 1.0, 0.0, 1.0);
    }
}
