use crate::buysell::Purchaser;
use async_trait::async_trait;
use pyo3::ffi::c_str;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyModule;
use pyo3::Python;

struct PythonPurchaser {

}

#[async_trait]
impl Purchaser for PythonPurchaser {
    async fn check_ticker_present(&self) -> bool {
        todo!()
    }

    async fn buy(&self, ticker: &str) -> anyhow::Result<()> {



        todo!()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::PyResult;
    use tokio::runtime::Runtime;

    #[test]
    fn test_python_call() {
        pyo3::prepare_freethreaded_python();

        let test: PyResult<()> = Python::with_gil(|py| {
            let result = py
                .eval(c_str!("[i * 10 for i in range(5)]"), None, None)
                .map_err(|e| {
                    e.print_and_set_sys_last_vars(py);
                }).expect("bad");
            let res: Vec<i64> = result.extract().unwrap();
            Ok(())
        });
    }

    
    fn test_python_invoke() {
        let res: PyResult<i32> = Python::with_gil(|py| {
            let modu = PyModule::import(py, "splitflow-buy.pyo3test")?;
            let test_function_return = modu.getattr("test_function")?.call0()?.extract::<i32>()?;
            
            Ok(test_function_return)
        });
        
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 42);

    }

    #[test]
    fn test_check_ticker_present() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let purchaser = PythonPurchaser {};
            assert!(purchaser.check_ticker_present().await);
        });
    }

    #[test]
    fn test_buy() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let purchaser = PythonPurchaser {};
            let result = purchaser.buy("AAPL").await;
            assert!(result.is_ok());
        });
    }
}