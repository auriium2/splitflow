use crate::buysell::MarketAccount;

struct Robinhood {

}

impl MarketAccount for Robinhood {
    async fn check_ticker_present() -> bool {
        todo!()
    }

    async fn buy(ticker: &str) {
        todo!()
    }

    async fn sell(ticker: &str) {
        todo!()
    }
}