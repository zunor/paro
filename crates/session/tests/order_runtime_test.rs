use paro_common::runtime_value::Value;
use paro_instance::{Instance, InstanceConfig};
use paro_session::{CollectingSink, Session};

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/unique_test_dir.rs"]
mod unique_test_dir;

use exec_ok::exec_ok;
use unique_test_dir::create_unique_test_dir;

async fn assert_order_by_name_returns_sorted_rows(
    session: &mut Session,
    sink: &mut CollectingSink,
) {
    exec_ok(
        session,
        sink,
        "CREATE TABLE order_test (id INT, name VARCHAR, score INT)",
    )
    .await;
    exec_ok(
        session,
        sink,
        "INSERT INTO order_test VALUES (3, 'C', 70), (1, 'A', 90), (2, 'B', 80)",
    )
    .await;
    exec_ok(session, sink, "SELECT * FROM order_test ORDER BY name").await;

    let result = sink.assert_single_result();
    let mut rows = Vec::new();
    for chunk in &result.chunks {
        for row_idx in 0..chunk.len() {
            rows.push((
                match chunk.column(0).unwrap().get_value(row_idx) {
                    Value::Integer(v) => v,
                    other => panic!("unexpected id value: {:?}", other),
                },
                match chunk.column(1).unwrap().get_value(row_idx) {
                    Value::Varchar(v) => v,
                    other => panic!("unexpected name value: {:?}", other),
                },
                match chunk.column(2).unwrap().get_value(row_idx) {
                    Value::Integer(v) => v,
                    other => panic!("unexpected score value: {:?}", other),
                },
            ));
        }
    }

    assert_eq!(
        rows,
        vec![
            (1, "A".to_string(), 90),
            (2, "B".to_string(), 80),
            (3, "C".to_string(), 70),
        ]
    );
}

#[tokio::test]
async fn order_by_name_returns_sorted_rows_for_in_memory_and_persistent_instances() {
    {
        let instance = Instance::new_in_memory();
        let mut session = Session::new(1, instance);
        let mut sink = CollectingSink::new();
        assert_order_by_name_returns_sorted_rows(&mut session, &mut sink).await;
    }

    let base_dir = create_unique_test_dir("order_runtime", "persistent");
    let config = InstanceConfig::new().with_instance_root(base_dir.to_string_lossy().to_string());
    let instance = Instance::new(config).expect("failed to create persistent instance");
    let mut session = Session::new(2, instance);
    let mut sink = CollectingSink::new();
    assert_order_by_name_returns_sorted_rows(&mut session, &mut sink).await;

    let _ = std::fs::remove_dir_all(&base_dir);
}
