use std::fmt;

use serde::{Deserialize, Serialize};

use crate::DurableError;

macro_rules! define_id {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            Serialize,
            Deserialize,
            utoipa::ToSchema,
        )]
        #[serde(transparent)]
        pub struct $name(i64);

        impl $name {
            pub fn new(value: i64) -> Result<Self, DurableError> {
                if value <= 0 {
                    return Err(DurableError::InvalidId(value));
                }
                Ok(Self(value))
            }

            pub fn get(self) -> i64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

define_id!(WorkflowId);
define_id!(ActivityId);
define_id!(ApprovalId);
define_id!(ScheduleRunId);
