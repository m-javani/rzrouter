// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzrouter.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use core::time;

use crate::common::{command::CommandResponse, helper::TestHelper};

mod common;

#[tokio::test]
async fn test_get_prop_room_day() {
    let mode = "edge"; // or "zone"
    let helper = TestHelper::new(mode).await;

    tokio::time::sleep(time::Duration::from_secs(1)).await;

    let result = helper.send_command("GETPROPROOMDAY").await;

    assert!(result.is_ok(), "Command should succeed: {:?}", result);

    let response = result.unwrap();
    let date = chrono::Utc::now()
        .date_naive()
        .checked_add_days(chrono::Days::new(1))
        .unwrap()
        .format("%Y-%m-%d")
        .to_string();
    match response {
        CommandResponse::GetPropRoomDay(resp) => {
            assert_eq!(resp.property_id, "s1_seg1_p1");
            assert_eq!(resp.date, date);
            assert!(resp.availability >= 1);
        }
    }
}
