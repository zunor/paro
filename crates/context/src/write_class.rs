#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteClass {
    #[default]
    Clean,
    HasDml,
    HasDdl,
    HasDmlAndDdl,
}
