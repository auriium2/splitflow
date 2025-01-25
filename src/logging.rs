/*

    He logs to console and then sends log to diskored, silly creature
 
pub struct ConsoleLogger {
    logger: Logger,
    tx: Sender<ConsoleCommand<u8>>
}

impl ConsoleLogger {
    pub fn new(tx: Sender<ConsoleCommand<u8>>) -> Self {
        let mut builder = pretty_env_logger::formatted_builder();

        if let Ok(s) = ::std::env::var("RUST_LOG") {
            builder.parse_filters(&s);
        }
        
        let logger = builder.build();
        let max_level = logger.filter();
        log::set_max_level(max_level);

        ConsoleLogger {
            logger,
            tx,
        }
    }
}

impl log::Log for ConsoleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) { 
        if self.enabled(record.metadata()) { 
            self.logger.log(record);
            //self.tx.try_send(ConsoleCommand::Print(ConsoleMessage::new(record.args().to_string()),false)).unwrap();
        }
    }

    fn flush(&self) {
        
    }
}*/