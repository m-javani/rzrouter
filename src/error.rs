// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzrouter.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum RZError {
    #[error("parse error: {0}")]
    ParseError(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("HTTP error: {0}")]
    Http(String),

    #[error("resolver error: {0}")]
    Resolver(String),

    #[error("no route for segment: {0}")]
    NoRoute(String),

    #[error("timeout")]
    Timeout,

    #[error("connection closed")]
    ConnectionClosed,

    #[error("validation error: {0}")]
    Validation(String),

    #[error("system error: {0}")]
    System(String),

    #[error("forward error: {0}")]
    Forward(String),
}

impl From<std::io::Error> for RZError {
    fn from(e: std::io::Error) -> Self {
        RZError::Io(e.to_string())
    }
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ForwardError {
    #[error("unknown segment: {0}")]
    UnknownSegment(String),

    #[error("network error")]
    NetworkError,

    #[error("timeout")]
    Timeout,

    #[error("internal error: {0}")]
    Internal(String),
}
