// tests/bench_rawvalue.rs — ad-hoc timing for the RawValue read-path
// spike: heavy items (dense geometry + many assets) through POST
// /catalogs/{id}/search, timed on raw response bytes (no client-side
// parse — measures handler + store work only). Ignored by default;
// run manually: cargo test --test bench_rawvalue -- --ignored --nocapture
mod common;

use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use common::*;
use serde_json::{json, Value};
use stac_os_rs::build_app;
use std::time::{Duration, Instant};
use tower::ServiceExt;

/// ~80KB item: 2000-vertex polygon, 40 COG-style assets, 60 properties.
fn heavy_item(id: &str, col: &str) -> Value {
    let mut ring: Vec<Value> = (0..2000)
        .map(|i| {
            let t = i as f64 * std::f64::consts::TAU / 2000.0;
            json!([t.cos() * 170.0, t.sin() * 80.0])
        })
        .collect();
    ring.push(ring[0].clone()); // geo_shape requires closed rings
    let mut assets = serde_json::Map::new();
    for i in 0..40 {
        assets.insert(
            format!("asset-{i:02}"),
            json!({
                "href": format!("https://example.com/data/{id}/band{i:02}.tif"),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": format!("Band {i}"),
                "roles": ["data"],
                "eo:bands": [{"name": format!("b{i}"), "center_wavelength": 0.5 + i as f64 * 0.01}],
                "proj:shape": [10980, 10980],
                "raster:bands": [{"nodata": 0, "data_type": "uint16",
                    "stats": {"min": 1, "max": 65535, "mean": 12345.6, "stddev": 5432.1}}]
            }),
        );
    }
    let mut properties = serde_json::Map::new();
    properties.insert("datetime".to_string(), json!("2024-01-01T00:00:00Z"));
    for i in 0..60 {
        properties.insert(
            format!("prop:{i:02}"),
            json!(format!("value-{i}-{}", "x".repeat(40))),
        );
    }
    json!({
        "type": "Feature",
        "id": id,
        "stac_version": "1.0.0",
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "bbox": [-100.0, -80.0, 100.0, 80.0],
        "properties": properties,
        "assets": assets,
        "collection": col,
    })
}

#[tokio::test]
#[ignore]
async fn bench_search_read_path() {
    let Some(state) = test_state(true).await else {
        eprintln!("skipping: OpenSearch unreachable");
        return;
    };
    let app = build_app(state.clone());
    let cat = uniq("bench-cat");
    let col = uniq("bench-col");
    call(&app, "POST", "/catalogs", Some(catalog(&cat))).await;
    call(
        &app,
        "POST",
        &format!("/catalogs/{cat}/collections"),
        Some(collection(&col)),
    )
    .await;

    const TOTAL: usize = 300;
    let ops: Vec<(String, Value, bool)> = (0..TOTAL)
        .map(|i| {
            let id = format!("bench-item-{i:04}");
            (format!("{id}|{col}"), heavy_item(&id, &col), true)
        })
        .collect();
    eprintln!("item size: {} bytes", ops[0].1.to_string().len());
    for chunk in ops.chunks(200) {
        // ~20MB per bulk — OpenSearch rejects bodies >100MB (413)
        let results = state
            .store
            .bulk_write(stac_os_rs::store::ITEMS_INDEX, chunk)
            .await
            .unwrap();
        assert!(results.iter().all(|r| r.status == 201));
    }

    async fn search(app: &axum::Router, cat: &str, limit: u64) -> usize {
        let req = Request::builder()
            .method("POST")
            .uri(format!("/catalogs/{cat}/search"))
            .header("content-type", "application/json")
            .body(Body::from(json!({"limit": limit}).to_string()))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            status,
            axum::http::StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&bytes)
        );
        bytes.len()
    }

    for limit in [50u64, 200] {
        for _ in 0..3 {
            search(&app, &cat, limit).await; // warmup
        }
        let mut times = Vec::new();
        let mut body_len = 0usize;
        for _ in 0..20 {
            let t = Instant::now();
            body_len = search(&app, &cat, limit).await;
            times.push(t.elapsed());
        }
        times.sort();
        let mean = times.iter().sum::<Duration>() / times.len() as u32;
        eprintln!(
            "limit={limit}: mean={mean:?} p50={:?} min={:?} max={:?} body={}B",
            times[times.len() / 2],
            times[0],
            times[times.len() - 1],
            body_len,
        );
    }
}
