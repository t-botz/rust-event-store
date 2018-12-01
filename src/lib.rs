extern crate serde_derive;

extern crate serde;
extern crate serde_json;

use std::error::Error;

trait EventStreamProcessor<T> {
    fn process_event(&self, event: T) -> Result<(), Box<Error>>;
}

trait EventCollector<S, T> {
    fn collect(&self, selector: S, store: &EventStreamProcessor<T>) -> Result<(), Box<Error>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_derive::Serialize;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::Path;

    struct StatefulJsonFileStore<'a> {
        state_path: &'a Path,
        data_path: &'a Path,
    }

    #[derive(Serialize)]
    struct EventCollectionResult<T, S> {
        events_collected: Vec<T>,
        state: S,
    }

    impl<'a, T, S> EventStreamProcessor<EventCollectionResult<T, S>> for StatefulJsonFileStore<'a>
    where
        T: serde::Serialize,
        S: serde::Serialize,
    {
        fn process_event(&self, event: EventCollectionResult<T, S>) -> Result<(), Box<Error>> {
            let data_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.data_path)
                .map_err(Box::new)?;
            let state_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(self.state_path)
                .map_err(Box::new)?;

            println!("Write data");
            serde_json::to_writer(&data_file, &event.events_collected).map_err(Box::new)?;
            (&data_file).write(b"\n")?;
            println!("Write state");
            serde_json::to_writer(state_file, &event.state).map_err(Box::new)?;
            Ok(())
        }
    }

    fn collect_from_last_state<'a, S, T>(
        store: &StatefulJsonFileStore<'a>,
        collector: &EventCollector<S, EventCollectionResult<T, S>>,
    ) -> Result<(), Box<Error>>
    where
        T: serde::Serialize,
        S: serde::Serialize + Default,
        for<'de> S: serde::Deserialize<'de>,
    {
        println!("Read Last state");
        let laste_state = if store.state_path.exists() {
            println!("Read from file");
            let state_file = OpenOptions::new()
                .read(true)
                .open(store.state_path)
                .map_err(Box::new)?;
            println!("Deserializing");
            serde_json::from_reader(state_file).map_err(Box::new)?
        } else {
            println!("Gettig default");
            S::default()
        };

        println!("Trigger collection");
        collector.collect(laste_state, store)?;
        Ok(())
    }

    #[test]
    fn it_works() -> Result<(), Box<Error>> {
        struct CollectorOfInt;
        // struct CollectorOfSubEvent;
        impl<'a> EventCollector<u32, EventCollectionResult<u32, u32>> for CollectorOfInt {
            fn collect(
                &self,
                selector: u32,
                store: &EventStreamProcessor<EventCollectionResult<u32, u32>>,
            ) -> Result<(), Box<Error>> {
                let mut my_vec = vec![];
                for i in (selector + 1)..10 {
                    my_vec.push(i);
                }
                println!("Send Results");
                store.process_event(EventCollectionResult {
                    events_collected: my_vec,
                    state: 10,
                })?;
                Ok(())
            }
        }

        let store = StatefulJsonFileStore {
            data_path: Path::new("result.json"),
            state_path: Path::new("state.json"),
        };
        collect_from_last_state(&store, &CollectorOfInt)
    }
}
