//! Experimental contracts for the enterprise migration design.
//!
//! This module is a fixture-backed executable spike. It proves interactions
//! between the plan, snapshot, journal, execution, resume, and verification
//! contracts. It does not provide a production database adapter.

pub mod artifact;
pub mod canonical;
pub mod connection;
pub mod fixture;
pub mod journal;
pub mod model;
pub mod plan;
pub mod runner;
pub mod verify;

pub const SPIKE_WARNING: &str = "EXPERIMENTAL SPIKE — NOT FOR PRODUCTION";
