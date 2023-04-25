use std::collections::HashMap;
use std::sync::PoisonError;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

pub struct DCS {
    store: RwLock<HashMap<String, String>>,
    list_store: RwLock<HashMap<String, Vec<String>>>,
}

impl DCS {
    pub fn new() -> Self {
        DCS {
            store: RwLock::new(HashMap::new()),
            list_store: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(
        &self,
        key: String,
        value: String,
    ) -> Result<(), PoisonError<RwLockWriteGuard<HashMap<String, String>>>> {
        let mut store = self.store.write()?;
        store.insert(key, value);
        Ok(())
    }

    pub fn get(
        &self,
        key: &str,
    ) -> Result<Option<String>, PoisonError<RwLockReadGuard<HashMap<String, String>>>> {
        let store = self.store.read()?;
        Ok(store.get(key).cloned())
    }

    pub fn list_push(
        &self,
        key: String,
        value: String,
    ) -> Result<usize, PoisonError<RwLockWriteGuard<HashMap<String, Vec<String>>>>> {
        let mut list_store = self.list_store.write()?;
        let list = list_store.entry(key).or_insert_with(Vec::new);
        list.push(value);
        Ok(list.len())
    }

    pub fn list_pop(
        &self,
        key: &str,
    ) -> Result<Option<String>, PoisonError<RwLockWriteGuard<HashMap<String, Vec<String>>>>> {
        let mut list_store = self.list_store.write()?;
        match list_store.get_mut(key) {
            Some(list) => Ok(list.pop()),
            None => Ok(None),
        }
    }

    pub fn list_len(
        &self,
        key: &str,
    ) -> Result<usize, PoisonError<RwLockReadGuard<HashMap<String, Vec<String>>>>> {
        let list_store = self.list_store.read()?;
        Ok(list_store.get(key).map_or(0, |list| list.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::DCS;

    #[test]
    fn test_set_get() {
        let dcs = DCS::new();
        dcs.set("key1".to_string(), "value1".to_string()).unwrap();
        assert_eq!(dcs.get("key1").unwrap(), Some("value1".to_string()));
    }

    #[test]
    fn test_get_nonexistent() {
        let dcs = DCS::new();
        assert_eq!(dcs.get("nonexistent_key").unwrap(), None);
    }

    #[test]
    fn test_list_push_pop() {
        let dcs = DCS::new();
        dcs.list_push("list1".to_string(), "value1".to_string())
            .unwrap();
        dcs.list_push("list1".to_string(), "value2".to_string())
            .unwrap();
        assert_eq!(dcs.list_pop("list1").unwrap(), Some("value2".to_string()));
        assert_eq!(dcs.list_pop("list1").unwrap(), Some("value1".to_string()));
        assert_eq!(dcs.list_pop("list1").unwrap(), None);
    }

    #[test]
    fn test_list_len() {
        let dcs = DCS::new();
        assert_eq!(dcs.list_len("list1").unwrap(), 0);
        dcs.list_push("list1".to_string(), "value1".to_string())
            .unwrap();
        dcs.list_push("list1".to_string(), "value2".to_string())
            .unwrap();
        assert_eq!(dcs.list_len("list1").unwrap(), 2);
    }
}
