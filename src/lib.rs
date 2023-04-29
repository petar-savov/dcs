use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::PoisonError;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

pub struct DCS {
    store: RwLock<HashMap<String, String>>,
    list_store: RwLock<HashMap<String, Vec<String>>>,
    hash_store: RwLock<HashMap<String, HashMap<String, String>>>,
    set_store: RwLock<HashMap<String, HashSet<String>>>,
    zset_store: RwLock<HashMap<String, BTreeMap<String, f64>>>,
}

impl DCS {
    pub fn new() -> Self {
        DCS {
            store: RwLock::new(HashMap::new()),
            list_store: RwLock::new(HashMap::new()),
            hash_store: RwLock::new(HashMap::new()),
            set_store: RwLock::new(HashMap::new()),
            zset_store: RwLock::new(HashMap::new()),
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

    pub fn list_push_multi(
        &self,
        key: String,
        values: Vec<String>,
    ) -> Result<usize, PoisonError<RwLockWriteGuard<HashMap<String, Vec<String>>>>> {
        let mut list_store = self.list_store.write()?;
        let list = list_store.entry(key).or_insert_with(Vec::new);
        list.extend(values);
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

    pub fn hash_set(
        &self,
        key: String,
        field: String,
        value: String,
    ) -> Result<(), PoisonError<RwLockWriteGuard<HashMap<String, HashMap<String, String>>>>> {
        let mut hash_store = self.hash_store.write()?;
        let hash = hash_store.entry(key).or_insert_with(HashMap::new);
        hash.insert(field, value);
        Ok(())
    }

    pub fn hash_get(
        &self,
        key: &str,
        field: &str,
    ) -> Result<
        Option<String>,
        PoisonError<RwLockReadGuard<HashMap<String, HashMap<String, String>>>>,
    > {
        let hash_store = self.hash_store.read()?;
        if let Some(hash) = hash_store.get(key) {
            Ok(hash.get(field).cloned())
        } else {
            Ok(None)
        }
    }

    pub fn hash_del(
        &self,
        key: String,
        field: String,
    ) -> Result<bool, PoisonError<RwLockWriteGuard<HashMap<String, HashMap<String, String>>>>> {
        let mut hash_store = self.hash_store.write()?;
        if let Some(hash) = hash_store.get_mut(&key) {
            Ok(hash.remove(&field).is_some())
        } else {
            Ok(false)
        }
    }

    pub fn set_add(
        &self,
        key: String,
        value: String,
    ) -> Result<(), PoisonError<RwLockWriteGuard<HashMap<String, HashSet<String>>>>> {
        let mut set_store = self.set_store.write()?;
        let set = set_store.entry(key).or_insert_with(HashSet::new);
        set.insert(value);
        Ok(())
    }

    pub fn set_is_member(
        &self,
        key: &str,
        value: &str,
    ) -> Result<bool, PoisonError<RwLockReadGuard<HashMap<String, HashSet<String>>>>> {
        let set_store = self.set_store.read()?;
        if let Some(set) = set_store.get(key) {
            Ok(set.contains(value))
        } else {
            Ok(false)
        }
    }

    pub fn set_remove(
        &self,
        key: String,
        value: String,
    ) -> Result<bool, PoisonError<RwLockWriteGuard<HashMap<String, HashSet<String>>>>> {
        let mut set_store = self.set_store.write()?;
        if let Some(set) = set_store.get_mut(&key) {
            Ok(set.remove(&value))
        } else {
            Ok(false)
        }
    }

    pub fn zset_add(
        &self,
        key: String,
        score: f64,
        value: String,
    ) -> Result<(), PoisonError<RwLockWriteGuard<HashMap<String, BTreeMap<String, f64>>>>> {
        let mut zset_store = self.zset_store.write()?;
        let zset = zset_store.entry(key).or_insert_with(BTreeMap::new);
        zset.insert(value, score);
        Ok(())
    }

    pub fn zset_score(
        &self,
        key: &str,
        value: &str,
    ) -> Result<Option<f64>, PoisonError<RwLockReadGuard<HashMap<String, BTreeMap<String, f64>>>>>
    {
        let zset_store = self.zset_store.read()?;
        if let Some(zset) = zset_store.get(key) {
            Ok(zset.get(value).cloned())
        } else {
            Ok(None)
        }
    }

    pub fn zset_remove(
        &self,
        key: String,
        value: String,
    ) -> Result<bool, PoisonError<RwLockWriteGuard<HashMap<String, BTreeMap<String, f64>>>>> {
        let mut zset_store = self.zset_store.write()?;
        if let Some(zset) = zset_store.get_mut(&key) {
            Ok(zset.remove(&value).is_some())
        } else {
            Ok(false)
        }
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

    #[test]
    fn test_list_push_multi() {
        let dcs = DCS::new();
        let values = vec![
            "value1".to_string(),
            "value2".to_string(),
            "value3".to_string(),
        ];
        dcs.list_push_multi("list1".to_string(), values).unwrap();
        assert_eq!(dcs.list_len("list1").unwrap(), 3);
    }

    #[test]
    fn test_hash_set_get() {
        let dcs = DCS::new();
        dcs.hash_set(
            "hash1".to_string(),
            "field1".to_string(),
            "value1".to_string(),
        )
        .unwrap();
        assert_eq!(
            dcs.hash_get("hash1", "field1").unwrap(),
            Some("value1".to_string())
        );
    }

    #[test]
    fn test_hash_del() {
        let dcs = DCS::new();
        dcs.hash_set(
            "hash1".to_string(),
            "field1".to_string(),
            "value1".to_string(),
        )
        .unwrap();
        assert_eq!(
            dcs.hash_del("hash1".to_string(), "field1".to_string())
                .unwrap(),
            true
        );
        assert_eq!(dcs.hash_get("hash1", "field1").unwrap(), None);
    }

    #[test]
    fn test_set_add_is_member() {
        let dcs = DCS::new();
        dcs.set_add("set1".to_string(), "value1".to_string())
            .unwrap();
        assert_eq!(dcs.set_is_member("set1", "value1").unwrap(), true);
        assert_eq!(dcs.set_is_member("set1", "value2").unwrap(), false);
    }

    #[test]
    fn test_set_remove() {
        let dcs = DCS::new();
        dcs.set_add("set1".to_string(), "value1".to_string())
            .unwrap();
        assert_eq!(
            dcs.set_remove("set1".to_string(), "value1".to_string())
                .unwrap(),
            true
        );
        assert_eq!(dcs.set_is_member("set1", "value1").unwrap(), false);
    }

    #[test]
    fn test_zset_add_score() {
        let dcs = DCS::new();
        dcs.zset_add("zset1".to_string(), 1.0, "value1".to_string())
            .unwrap();
        assert_eq!(dcs.zset_score("zset1", "value1").unwrap(), Some(1.0));
        assert_eq!(dcs.zset_score("zset1", "value2").unwrap(), None);
    }

    #[test]
    fn test_zset_remove() {
        let dcs = DCS::new();
        dcs.zset_add("zset1".to_string(), 1.0, "value1".to_string())
            .unwrap();
        assert_eq!(
            dcs.zset_remove("zset1".to_string(), "value1".to_string())
                .unwrap(),
            true
        );
        assert_eq!(dcs.zset_score("zset1", "value1").unwrap(), None);
    }
}
