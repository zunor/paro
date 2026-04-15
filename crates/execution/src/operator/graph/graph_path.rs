use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

pub const PATH_HIDDEN_COLUMN_COUNT: usize = 3;
pub const PATH_LENGTH_OFFSET: usize = 0;
pub const PATH_VERTICES_OFFSET: usize = 1;
pub const PATH_EDGES_OFFSET: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathRowRefSpec {
    pub table_oid: u64,
    pub rowid_col_idx: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PathEmitSpec {
    pub prefix_vertices: Vec<PathRowRefSpec>,
    pub prefix_edges: Vec<PathRowRefSpec>,
    pub segment_vertex_table_oid: u64,
    pub segment_edge_table_oid: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathElementRef {
    pub table_oid: u64,
    pub rowid: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaterializedPath {
    pub length: i64,
    pub vertices: Vec<PathElementRef>,
    pub edges: Vec<PathElementRef>,
}

pub fn path_element_struct_type() -> LogicalType {
    LogicalType::Struct(vec![
        ("table_oid".to_string(), LogicalType::UBigInt),
        ("rowid".to_string(), LogicalType::UBigInt),
    ])
}

pub fn path_element_list_type() -> LogicalType {
    LogicalType::List(Box::new(path_element_struct_type()))
}

pub fn collect_prefix_path(input: &Chunk, row: usize, spec: &PathEmitSpec) -> MaterializedPath {
    let mut path = MaterializedPath {
        length: spec.prefix_edges.len() as i64,
        vertices: Vec::with_capacity(spec.prefix_vertices.len()),
        edges: Vec::with_capacity(spec.prefix_edges.len()),
    };

    for vertex in &spec.prefix_vertices {
        path.vertices.push(PathElementRef {
            table_oid: vertex.table_oid,
            rowid: input
                .column(vertex.rowid_col_idx)
                .and_then(|col| col.get_u64(row))
                .unwrap_or(0),
        });
    }

    for edge in &spec.prefix_edges {
        path.edges.push(PathElementRef {
            table_oid: edge.table_oid,
            rowid: input
                .column(edge.rowid_col_idx)
                .and_then(|col| col.get_u64(row))
                .unwrap_or(0),
        });
    }

    path
}

pub fn materialize_path_vectors(
    paths: &[MaterializedPath],
) -> (Arc<Vector>, Arc<Vector>, Arc<Vector>) {
    let mut length_vec = Vector::with_capacity(LogicalType::BigInt, paths.len());
    length_vec.set_len(paths.len());

    let vertex_lists = paths
        .iter()
        .map(|path| path.vertices.as_slice())
        .collect::<Vec<_>>();
    let edge_lists = paths
        .iter()
        .map(|path| path.edges.as_slice())
        .collect::<Vec<_>>();

    for (idx, path) in paths.iter().enumerate() {
        length_vec.set_i64(idx, path.length);
    }

    (
        Arc::new(length_vec),
        Arc::new(build_path_list_vector(&vertex_lists)),
        Arc::new(build_path_list_vector(&edge_lists)),
    )
}

fn build_path_list_vector(rows: &[&[PathElementRef]]) -> Vector {
    let list_type = path_element_list_type();
    let struct_type = path_element_struct_type();
    let total_children = rows.iter().map(|row| row.len()).sum::<usize>();

    let mut list_vec = Vector::with_capacity(list_type, rows.len());
    list_vec.set_len(rows.len());

    let mut child_vec = Vector::with_capacity(struct_type, total_children.max(1));
    child_vec.set_count(total_children);
    let children = child_vec
        .children_mut()
        .expect("Path child vector must be a struct");
    let (table_oid_children, rowid_children) = children.split_at_mut(1);
    let table_oid_child = Arc::make_mut(&mut table_oid_children[0]);
    let rowid_child = Arc::make_mut(&mut rowid_children[0]);

    let mut child_offset = 0usize;
    for (row_idx, refs) in rows.iter().enumerate() {
        unsafe {
            write_list_entry(
                &mut list_vec,
                row_idx,
                child_offset as u32,
                refs.len() as u32,
            );
        }
        list_vec.set_null(row_idx, false);

        for (elem_idx, elem) in refs.iter().enumerate() {
            let out_idx = child_offset + elem_idx;
            table_oid_child.set_u64(out_idx, elem.table_oid);
            rowid_child.set_u64(out_idx, elem.rowid);
        }
        child_offset += refs.len();
    }

    list_vec.set_child(Arc::new(child_vec));
    list_vec
}

unsafe fn write_list_entry(vector: &mut Vector, idx: usize, offset: u32, length: u32) {
    let base = vector.flat_data_mut::<u8>();
    let ptr = base.add(idx * 8) as *mut u32;
    std::ptr::write_unaligned(ptr, offset);
    std::ptr::write_unaligned(ptr.add(1), length);
}

pub fn path_elements_to_value(elements: &[PathElementRef]) -> Value {
    let fields = vec![
        ("table_oid".to_string(), LogicalType::UBigInt),
        ("rowid".to_string(), LogicalType::UBigInt),
    ];
    Value::list(
        path_element_struct_type(),
        elements
            .iter()
            .map(|element| {
                Value::struct_value(
                    fields.clone(),
                    vec![
                        Value::UBigInt(element.table_oid),
                        Value::UBigInt(element.rowid),
                    ],
                )
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialize_list_vector_roundtrip() {
        let paths = vec![
            MaterializedPath {
                length: 1,
                vertices: vec![
                    PathElementRef {
                        table_oid: 11,
                        rowid: 101,
                    },
                    PathElementRef {
                        table_oid: 11,
                        rowid: 102,
                    },
                ],
                edges: vec![PathElementRef {
                    table_oid: 22,
                    rowid: 201,
                }],
            },
            MaterializedPath {
                length: 0,
                vertices: vec![PathElementRef {
                    table_oid: 11,
                    rowid: 103,
                }],
                edges: vec![],
            },
        ];

        let (_len, vertices, edges) = materialize_path_vectors(&paths);
        assert_eq!(
            vertices.get_value(0).to_string(),
            path_elements_to_value(&paths[0].vertices).to_string()
        );
        assert_eq!(
            vertices.get_value(1).to_string(),
            path_elements_to_value(&paths[1].vertices).to_string()
        );
        assert_eq!(
            edges.get_value(0).to_string(),
            path_elements_to_value(&paths[0].edges).to_string()
        );
        assert_eq!(
            edges.get_value(1).to_string(),
            path_elements_to_value(&paths[1].edges).to_string()
        );
    }
}
