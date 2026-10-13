use std::io;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail};
use reqwest::Url;
use tempfile::NamedTempFile;

use crate::html;

pub fn get_local_path(filename: &str) -> anyhow::Result<Box<dyn AsRef<Path>>> {
    if filename.starts_with("https://") || filename.starts_with("http://") {
        match download_asciicast(filename) {
            Ok(path) => Ok(Box::new(path)),
            Err(e) => bail!(anyhow!("download failed: {e}")),
        }
    } else {
        Ok(Box::new(PathBuf::from(filename)))
    }
}

fn download_asciicast(url: &str) -> anyhow::Result<NamedTempFile> {
    use reqwest::blocking::{Client, Response};
    use reqwest::redirect::Policy;

    let client = Client::builder()
        .timeout(None)
        .redirect(Policy::none())
        .build()?;
    let parsed_url = Url::parse(url)?;

    let fetch_following_redirects = |start_url: Url| -> anyhow::Result<Response> {
        let mut current = start_url;
        for _ in 0..10 {
            let mut req = client.get(current.clone());
            let is_gcs = current
                .host_str()
                .is_some_and(|h| h.eq_ignore_ascii_case("storage.googleapis.com"));
            if !is_gcs {
                if let Some((hdr_name, hdr_val)) = crate::api::get_transport_auth_header() {
                    req = req.header(hdr_name, hdr_val);
                }
            }
            let resp = req.send()?;
            if resp.status().is_redirection() {
                if let Some(loc) = resp.headers().get(reqwest::header::LOCATION) {
                    let loc_str = loc.to_str()?;
                    current = current
                        .join(loc_str)
                        .or_else(|_| Url::parse(loc_str))?;
                    continue;
                }
            }
            resp.error_for_status_ref()?;
            return Ok(resp);
        }
        bail!("too many redirects while downloading {url}")
    };

    let mut response = fetch_following_redirects(parsed_url.clone())?;
    let mut file = NamedTempFile::new()?;

    let content_type = response
        .headers()
        .get("content-type")
        .ok_or(anyhow!("no content-type header in the response"))?
        .to_str()?
        .to_owned();

    if content_type.starts_with("text/html") {
        let base_url = response.url().clone();
        let html_body = response.text()?;

        if let Some(href) = html::extract_asciicast_link(&html_body) {
            let cast_url = base_url
                .join(&href)
                .or_else(|_| parsed_url.join(&href))?;
            let mut response = fetch_following_redirects(cast_url)?;
            io::copy(&mut response, &mut file)?;

            Ok(file)
        } else {
            bail!(
                r#"<link rel="alternate" type="application/x-asciicast" href="..."> not found in the HTML page"#
            );
        }
    } else {
        io::copy(&mut response, &mut file)?;

        Ok(file)
    }
}

pub struct Utf8Decoder(Vec<u8>);

impl Utf8Decoder {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn feed(&mut self, input: &[u8]) -> String {
        let mut output = String::new();
        self.0.extend_from_slice(input);

        while !self.0.is_empty() {
            match std::str::from_utf8(&self.0) {
                Ok(valid_str) => {
                    output.push_str(valid_str);
                    self.0.clear();
                    break;
                }

                Err(e) => {
                    let n = e.valid_up_to();
                    let valid_bytes: Vec<u8> = self.0.drain(..n).collect();
                    let valid_str = unsafe { std::str::from_utf8_unchecked(&valid_bytes) };
                    output.push_str(valid_str);

                    match e.error_len() {
                        Some(len) => {
                            self.0.drain(..len);
                            output.push('�');
                        }

                        None => {
                            break;
                        }
                    }
                }
            }
        }

        output
    }
}

/// Quantizer using error diffusion based on Bresenham algorithm.
/// It ensures the accumulated error at any point is less than Q/2.
pub struct Quantizer {
    q: i128,
    error: i128,
}

impl Quantizer {
    pub fn new(q: u128) -> Self {
        Quantizer {
            q: q as i128,
            error: 0,
        }
    }

    pub fn next(&mut self, value: u128) -> u128 {
        let error_corrected_value = value as i128 + self.error;
        let steps = (error_corrected_value + self.q / 2) / self.q;
        let quantized_value = steps * self.q;

        self.error = error_corrected_value - quantized_value;
        debug_assert!((self.error).abs() <= self.q / 2);

        quantized_value as u128
    }
}

#[cfg(test)]
mod tests {
    use super::{Quantizer, Utf8Decoder};

    #[test]
    fn utf8_decoder() {
        let mut decoder = Utf8Decoder::new();

        assert_eq!(decoder.feed(b"czarna "), "czarna ");
        assert_eq!(decoder.feed(&[0xc5, 0xbc, 0xc3]), "ż");
        assert_eq!(decoder.feed(&[0xb3, 0xc5, 0x82]), "ół");
        assert_eq!(decoder.feed(&[0xc4]), "");
        assert_eq!(decoder.feed(&[0x87, 0x21]), "ć!");
        assert_eq!(decoder.feed(&[0x80]), "�");
        assert_eq!(decoder.feed(&[]), "");
        assert_eq!(decoder.feed(&[0x80, 0x81]), "��");
        assert_eq!(decoder.feed(&[]), "");
        assert_eq!(decoder.feed(&[0x23]), "#");
        assert_eq!(
            decoder.feed(&[0x83, 0x23, 0xf0, 0x90, 0x80, 0xc0, 0x21]),
            "�#��!"
        );
    }

    #[test]
    fn quantizer() {
        let mut quantizer = Quantizer::new(1_000);

        let input = [
            26692, 540290, 64736, 105951, 171006, 191943, 107942, 128108, 148904, 108973, 211002,
            44701, 489307, 405987, 105028, 194590, 61043, 532296, 319015, 152786, 32578, 5445,
            40542, 756,
        ];

        let expected = [
            27000, 540000, 65000, 106000, 171000, 192000, 108000, 128000, 149000, 109000, 211000,
            44000, 490000, 406000, 105000, 194000, 61000, 532000, 320000, 152000, 33000, 5000,
            41000, 1000,
        ];

        let mut quantized = Vec::new();
        let mut input_sum = 0;
        let mut quantized_sum = 0;

        for input_value in input {
            let quantized_value = quantizer.next(input_value);
            quantized.push(quantized_value);
            input_sum += input_value;
            quantized_sum += quantized_value;
            let error = (input_sum as i128 - quantized_sum as i128).abs();

            assert!(error <= 500, "error: {error}");
        }

        assert_eq!(quantized, expected);
    }

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    use axum::extract::State;
    use axum::http::{header, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use axum::Router;
    use tokio::net::TcpListener;
    use tokio::runtime::Runtime;
    use tokio::sync::oneshot;

    use crate::asciicast::{self, EventData, Version};

    #[derive(Default)]
    struct DownloadMockState {
        html_hits: AtomicUsize,
        cast_redirect_hits: AtomicUsize,
        gcs_hits: AtomicUsize,
        server_url: std::sync::OnceLock<String>,
    }

    #[test]
    fn download_asciicast_resolves_relative_link_and_follows_302_redirect_to_gcs() {
        let state = Arc::new(DownloadMockState::default());
        let state_for_server = Arc::clone(&state);
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server_thread = thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let base_url = format!("http://{addr}");
                let _ = state_for_server.server_url.set(base_url);
                addr_tx.send(addr).unwrap();

                let app = Router::new()
                    .route(
                        "/a/test",
                        get(|State(st): State<Arc<DownloadMockState>>| async move {
                            st.html_hits.fetch_add(1, Ordering::SeqCst);
                            let html = r#"<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <link rel="alternate" type="application/x-asciicast" href="/a/test.cast">
</head>
<body>Recording Page</body>
</html>"#;
                            ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html)
                        }),
                    )
                    .route(
                        "/a/test.cast",
                        get(|State(st): State<Arc<DownloadMockState>>| async move {
                            st.cast_redirect_hits.fetch_add(1, Ordering::SeqCst);
                            let base = st.server_url.get().unwrap();
                            let gcs_url = format!(
                                "{base}/gcs/bucket/recordings/test.cast?X-Goog-Algorithm=GOOG4-RSA-SHA256"
                            );
                            (
                                StatusCode::FOUND,
                                [
                                    (header::LOCATION, gcs_url),
                                    (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".to_owned()),
                                ],
                                "",
                            )
                                .into_response()
                        }),
                    )
                    .route(
                        "/gcs/bucket/recordings/test.cast",
                        get(|State(st): State<Arc<DownloadMockState>>| async move {
                            st.gcs_hits.fetch_add(1, Ordering::SeqCst);
                            let cast_body = concat!(
                                "{\"version\":3,\"term\":{\"cols\":100,\"rows\":50}}\n",
                                "[1.25, \"o\", \"hello from gcs redirect\"]\n"
                            );
                            (
                                [(header::CONTENT_TYPE, "application/x-asciicast")],
                                cast_body,
                            )
                        }),
                    )
                    .with_state(state_for_server);

                let _ = axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await;
            });
        });

        let addr = addr_rx.recv().unwrap();
        let page_url = format!("http://{addr}/a/test");

        let local_path = super::get_local_path(&page_url).unwrap();
        let cast = asciicast::open_from_path(&*local_path).unwrap();

        assert_eq!(cast.version, Version::Three);
        assert_eq!((cast.header.term_cols, cast.header.term_rows), (100, 50));

        let events = cast.events.collect::<anyhow::Result<Vec<_>>>().unwrap();
        assert_eq!(events.len(), 1);
        assert!(
            matches!(events[0].data, EventData::Output(ref s) if s == "hello from gcs redirect")
        );

        assert_eq!(state.html_hits.load(Ordering::SeqCst), 1);
        assert_eq!(state.cast_redirect_hits.load(Ordering::SeqCst), 1);
        assert_eq!(state.gcs_hits.load(Ordering::SeqCst), 1);

        // Also verify direct .cast URL download following 302 redirect
        let direct_cast_url = format!("http://{addr}/a/test.cast");
        let direct_local_path = super::get_local_path(&direct_cast_url).unwrap();
        let direct_cast = asciicast::open_from_path(&*direct_local_path).unwrap();
        assert_eq!(direct_cast.version, Version::Three);
        assert_eq!(state.cast_redirect_hits.load(Ordering::SeqCst), 2);
        assert_eq!(state.gcs_hits.load(Ordering::SeqCst), 2);

        let _ = shutdown_tx.send(());
        server_thread.join().unwrap();
    }
}
