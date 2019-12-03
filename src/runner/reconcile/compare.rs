use serde_json::Value;

use std::fmt::{self, Display, Write};

type JsonObject = serde_json::Map<String, Value>;

#[derive(Debug, PartialEq)]
pub struct Diff<'a> {
    pub path: String,
    pub existing: &'a Value,
    pub desired: &'a Value,
}

impl<'a> Display for Diff<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Diff at path: '{}', existing: {}, desired: {}",
            self.path, self.existing, self.desired
        )
    }
}

pub struct Diffs<'a>(Vec<Diff<'a>>);
impl<'a> Diffs<'a> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn non_empty(&self) -> bool {
        !self.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[cfg(feature = "testkit")]
    pub fn into_vec(self) -> Vec<Diff<'a>> {
        self.0
    }
}

impl<'a> Display for Diffs<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_empty() {
            f.write_str("<empty>")
        } else {
            write!(f, "{} differences: ", self.0.len())?;
            for (i, diff) in self.0.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                Display::fmt(diff, f)?;
            }
            Ok(())
        }
    }
}

enum Segment<'a> {
    Key(&'a str),
    Index(usize),
}

/// compares an existing and desired json value, each representing a Kubernetes resource.
/// The semantics of this comparison are very particular to our specific use case. This
/// function will return an empty diff as long as `existing` is a superset of `desired`.
/// In other words, if every value in `desired` is present in `existing`, then the diff will
/// be empty, but values from `desired` that are not included in `existing` will show up in
/// the diff.
///
/// Additionally, this function attempts to deal properly with "associative arrays", which
/// are an unfortunately common pattern in Kubernetes. Associative arrays model the data as
/// an array of objects instead of as an object. For example:
///
/// ```json
/// {
///     "as_associative_array": [
///         {
///             "name": "foo",
///             "value": "fooValue"
///         },
///         {
///             "name": "bar",
///             "value": "barValue"
///         }
///     ],
///     "as_object": {
///         "foo": "fooValue",
///         "bar": "barValue"
///     }
/// }
/// ```
///
/// Kubernetes sometimes uses associative arrays for things like environment variables, which makes
/// comparison of arrays difficult because the order is not relevant. At least for now, we use a
/// fairly simple way of detective whether a given array should be compared as an associative array
/// or a regular one (regular arrays simply check if all the elements are equal in order). If _all_
/// the items in the desired array are objects that contain a `name` field with a String value, then
/// we'll consider the array to be associative.
pub fn compare_values<'a>(existing: &'a Value, desired: &'a Value) -> Diffs<'a> {
    let mut diffs = Vec::new();
    let mut path = Vec::with_capacity(8);
    compare(&mut diffs, &mut path, existing, desired);
    Diffs(diffs)
}

fn compare<'a>(
    diffs: &mut Vec<Diff<'a>>,
    path: &mut Vec<Segment<'a>>,
    superset: &'a Value,
    subset: &'a Value,
) {
    match (superset, subset) {
        (Value::Object(ref super_map), Value::Object(ref sub_map)) => {
            compare_objects(diffs, path, super_map, sub_map);
        }
        (Value::Array(ref super_array), Value::Array(ref sub_array)) => {
            compare_arrays(diffs, path, super_array, sub_array);
        }
        (a, b) if a != b => {
            diffs.push(diff(&*path, a, b));
        }
        _ => {}
    }
}

fn compare_objects<'a>(
    diffs: &mut Vec<Diff<'a>>,
    path: &mut Vec<Segment<'a>>,
    existing: &'a JsonObject,
    desired: &'a JsonObject,
) {
    for (key, desired_val) in desired.iter() {
        check_value(diffs, path, existing, key, desired_val);
    }
}

fn compare_arrays<'a>(
    diffs: &mut Vec<Diff<'a>>,
    path: &mut Vec<Segment<'a>>,
    existing: &'a Vec<Value>,
    desired: &'a Vec<Value>,
) {
    if is_associative(existing, desired) {
        compare_associative_arrays(diffs, path, existing, desired);
    } else {
        compare_non_associative_arrays(diffs, path, existing, desired);
    }
}

fn compare_non_associative_arrays<'a>(
    diffs: &mut Vec<Diff<'a>>,
    path: &mut Vec<Segment<'a>>,
    existing: &'a Vec<Value>,
    desired: &'a Vec<Value>,
) {
    for (i, desired_item) in desired.iter().enumerate() {
        path.push(Segment::Index(i));
        if existing.len() > i {
            compare(diffs, path, &existing[i], desired_item);
        } else {
            diffs.push(diff(&*path, &Value::Null, desired_item));
        }
        path.pop();
    }
}

fn compare_associative_arrays<'a>(
    diffs: &mut Vec<Diff<'a>>,
    path: &mut Vec<Segment<'a>>,
    existing: &'a Vec<Value>,
    desired: &'a Vec<Value>,
) {
    for (i, desired_val) in desired.iter().enumerate() {
        path.push(Segment::Index(i));
        // These are only safe unwraps because we check them in `is_associative`
        let name = desired_val.get("name").unwrap().as_str().unwrap();

        let existing_item = existing.iter().find(|e| {
            e.get("name")
                .and_then(Value::as_str)
                .map(|item_name| item_name == name)
                .unwrap_or(false)
        });
        if let Some(existing_match) = existing_item {
            compare(diffs, path, existing_match, desired_val);
        } else {
            diffs.push(diff(&*path, &Value::Null, desired_val));
        }
        path.pop();
    }
}

fn is_associative(_existing: &Vec<Value>, desired: &Vec<Value>) -> bool {
    desired.iter().all(|v| {
        v.as_object()
            .map(|o| o.get("name").map(Value::is_string).unwrap_or(false))
            .unwrap_or(false)
    })
}

fn check_value<'a>(
    diffs: &mut Vec<Diff<'a>>,
    path: &mut Vec<Segment<'a>>,
    existing: &'a JsonObject,
    key: &'a str,
    value: &'a Value,
) {
    path.push(Segment::Key(key));

    match existing.get(key) {
        Some(super_val) => {
            compare(diffs, path, super_val, value);
        }
        None => {
            diffs.push(diff(&*path, &Value::Null, value));
        }
    }
    path.pop();
}

fn diff<'a>(path: &Vec<Segment>, existing: &'a Value, desired: &'a Value) -> Diff<'a> {
    let mut p = String::with_capacity(8);
    for s in path.iter() {
        p.push('.');
        match s {
            Segment::Key(ref k) => p.push_str(k),
            Segment::Index(i) => {
                write!(p, "{}", i).unwrap();
            }
        }
    }
    Diff {
        path: p,
        existing,
        desired,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn returs_diffs_from_objects() {
        let existing = json! {{
            "key1": {
                "nested1": "same",
                "nested2": "existing2",
            },
            "key2": "here",
            "key3": 7,
        }};
        let desired = json! {{
            "key1": {
                "nested1": "same",
                "nested2": "desired2",
            },
            "key3": 8,
            "newKey": true,
        }};

        let diffs = compare_values(&existing, &desired);
        let existing2 = Value::String("existing2".to_owned());
        let desired2 = Value::String("desired2".to_owned());
        let seven = Value::Number(7.into());
        let eight = Value::Number(8.into());

        let expected = vec![
            Diff {
                path: ".key1.nested2".to_owned(),
                existing: &existing2,
                desired: &desired2,
            },
            Diff {
                path: ".key3".to_owned(),
                existing: &seven,
                desired: &eight,
            },
            Diff {
                path: ".newKey".to_owned(),
                existing: &Value::Null,
                desired: &Value::Bool(true),
            },
        ];
        assert_all_diffs_present(expected, diffs);
    }

    #[test]
    fn returns_diffs_from_nested_arrays() {
        let existing = json! {{
            "nonAssociative": [
                {
                    "nonAssociative": [
                        { "same": "same" },
                        { "same": "same" },
                        { "different": "e" },
                    ]
                }
            ],
            "associative": [
                {"name": "name2", "value": "value2"},
                {"name": "name3", "value": "value3"},
                {"name": "name1", "value": "value1existing"},
            ]
        }};
        let desired = json! {{
            "nonAssociative": [
                {
                    "nonAssociative": [
                        { "same": "same" },
                        { "same": "same" },
                        { "different": "desired" },
                    ]
                }
            ],
            "associative": [
                {"name": "name3", "value": "value3"},
                {"name": "name1", "value": "value1desired"},
                {"name": "name2", "value": "value2"},
            ]
        }};
        let e = json!("e");
        let desired_val = json!("desired");
        let value1existing = json!("value1existing");
        let value1desired = json!("value1desired");

        let expected = vec![
            Diff {
                path: ".nonAssociative.0.nonAssociative.2.different".to_owned(),
                existing: &e,
                desired: &desired_val,
            },
            Diff {
                path: ".associative.1.value".to_owned(),
                existing: &value1existing,
                desired: &value1desired,
            },
        ];
        let actual = compare_values(&existing, &desired);
        assert_all_diffs_present(expected, actual);
    }

    fn assert_all_diffs_present(expected: Vec<Diff>, mut actual: Diffs) {
        for expected_diff in expected.iter() {
            if !actual.0.contains(expected_diff) {
                panic!(
                    "Expected to find diff: {} in actual diffs: {}",
                    expected_diff, actual
                );
            }
        }
        actual.0.retain(|e| !expected.contains(e));
        if !actual.is_empty() {
            panic!(
                "Expected {} diffs but found {} extra, \nextra_diffs: {}\nexpected: {}",
                expected.len(),
                actual.len(),
                actual,
                Diffs(expected)
            );
        }
    }
}
