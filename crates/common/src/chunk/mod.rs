#[allow(clippy::module_inception)]
mod chunk;
mod ops;

#[cfg(test)]
mod tests;

pub use chunk::Chunk;
