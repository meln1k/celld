// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! `EpochChain::build` over real LTX objects: a paged epoch's marker names
//! the epoch it continued, and the chain links only that one below it.

use celld_ltx::client::epochs::EpochChain;
use celld_ltx::client::file::FileReplicaClient;
use celld_ltx::client::ReplicaClient;
use celld_ltx::ltx::{Header, HEADER_FLAG_NO_CHECKSUM, VERSION};
use celld_ltx::TXID;

/// Upload an object spanning `min..=max` at level 0 of `client`'s epoch.
/// The chain reads only listings and a marker's header, so a header is the
/// whole object.
async fn put(client: &FileReplicaClient, min: u64, max: u64, node_id: u64) {
    let header = Header {
        version: VERSION,
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size: 4096,
        commit: 1,
        min_txid: TXID(min),
        max_txid: TXID(max),
        node_id,
        ..Header::default()
    };
    (client.write_ltx_file(0, TXID(min), TXID(max), &header.marshal()))
        .await
        .unwrap();
}

/// The chain's spans over three epochs. Epoch 1 is the source: txids 1..=3.
/// Epoch 2 is a fenced owner's late snapshot of the same length, uploaded
/// after epoch 3 restored. Epoch 3 is paged: its marker at txid 4 names
/// `marker_names`, and it holds txids 5..=6.
async fn spans(marker_names: u64) -> Vec<(u64, TXID)> {
    let root = tempfile::tempdir().unwrap();
    let client = |epoch: u64| {
        FileReplicaClient::new(root.path().join(format!("e{epoch}")).to_str().unwrap())
    };
    put(&client(1), 1, 3, 0).await;
    put(&client(2), 1, 3, 0).await;
    put(&client(3), 4, 4, marker_names).await;
    put(&client(3), 5, 6, 0).await;
    let epochs = (1..=3).map(|epoch| (epoch, client(epoch))).collect();
    EpochChain::build(epochs).await.unwrap().spans()
}

#[tokio::test]
async fn a_paged_epoch_links_the_epoch_its_marker_names() {
    // Epoch 2 also ends at the cut, and it is newer, but the marker names 1.
    assert_eq!(spans(1).await, [(1, TXID(1)), (3, TXID(4))]);
}

#[tokio::test]
async fn a_marker_that_names_no_epoch_keeps_the_old_rule() {
    // Written before markers named their source: the newest epoch that ends
    // at the cut links, here the fenced one.
    assert_eq!(spans(0).await, [(2, TXID(1)), (3, TXID(4))]);
}
