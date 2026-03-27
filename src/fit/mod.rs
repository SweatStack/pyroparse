//! Custom FIT file parser for pyroparse.
//!
//! This module will eventually replace the `fitparser` dependency with a
//! purpose-built parser optimized for pyroparse's specific needs.
//!
//! ## Modules
//!
//! - `binary` — Zero-knowledge binary reader (headers, definitions, data)
//! - `decode` — Message decoder (metadata scan, eventually full parse)
//! - `profile` — Auto-generated message/field/enum definitions from FIT SDK

pub mod binary;
pub mod decode;
pub mod profile;
