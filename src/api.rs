use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::path::Path;

use anyhow::{bail, Context, Result};
use reqwest::{header, Body, Response};
use reqwest::{multipart::Form, Client, RequestBuilder};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_util::io::ReaderStream;
use url::Url;

use crate::config::Config;

const ASCIICAST_CONTENT_TYPE: &str = "application/x-asciicast";
const UPLOAD_STREAM_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug, Deserialize)]
pub struct RecordingResponse {
    pub url: String,
    pub message: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StreamResponse {
    pub id: u64,
    pub ws_producer_url: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    Public,
    Unlisted,
    Private,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RecordingChangeset {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility: Option<Visibility>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_url: Option<Option<String>>,
}

#[derive(Debug, Serialize)]
struct InitiateUploadRequest<'a> {
    filename: &'a str,
    byte_size: u64,
    content_type: &'a str,
    #[serde(flatten)]
    changeset: &'a RecordingChangeset,
}

#[derive(Debug, Deserialize)]
struct InitiateUploadResponse {
    mode: Option<String>,
    upload_id: Option<String>,
    object_key: Option<String>,
    upload_url: Option<String>,
    upload_method: Option<String>,
    #[allow(dead_code)]
    upload_type: Option<String>,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
    complete_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct CompleteUploadRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    upload_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_key: Option<&'a str>,
    filename: &'a str,
    byte_size: u64,
    #[serde(flatten)]
    changeset: &'a RecordingChangeset,
}

enum DirectUploadOutcome {
    Completed(RecordingResponse),
    FallbackToMultipart,
}

#[derive(Default, Serialize)]
pub struct StreamChangeset {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility: Option<Visibility>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_url: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub term_type: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub term_version: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shell: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<Option<HashMap<String, String>>>,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    #[serde(rename = "type")]
    error_type: Option<String>,
    message: Option<String>,
    details: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ErrorDetail {
    field: Option<String>,
    message: String,
}

pub fn get_auth_url(config: &mut Config) -> Result<Url> {
    let mut url = config.get_server_url()?;
    url.set_path(&format!("connect/{}", config.get_install_id()?));

    Ok(url)
}

pub async fn create_recording(
    path: &str,
    changeset: RecordingChangeset,
    config: &mut Config,
) -> Result<RecordingResponse> {
    let server_url = &config.get_server_url()?;
    let install_id = config.get_install_id()?;

    match try_direct_upload(server_url, &install_id, path, &changeset).await? {
        DirectUploadOutcome::Completed(response) => Ok(response),
        DirectUploadOutcome::FallbackToMultipart => {
            create_recording_multipart(server_url, install_id, path, changeset).await
        }
    }
}

async fn create_recording_multipart(
    server_url: &Url,
    install_id: String,
    path: &str,
    changeset: RecordingChangeset,
) -> Result<RecordingResponse> {
    let response = create_recording_request(server_url, install_id, path, changeset)
        .await?
        .send()
        .await?;

    let legacy_fallback = (response.status().as_u16() == 413)
        .then(|| "The recording exceeds the server-configured size limit".to_owned());

    let server_hostname = server_url
        .host()
        .expect("host presence is checked in parse_server_url")
        .to_string();
    let response = handle_response_status(response, &server_hostname, legacy_fallback).await?;

    Ok(response.json::<RecordingResponse>().await?)
}

async fn try_direct_upload(
    server_url: &Url,
    install_id: &str,
    path: &str,
    changeset: &RecordingChangeset,
) -> Result<DirectUploadOutcome> {
    let metadata = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("failed to read metadata for {path}"))?;
    let byte_size = metadata.len();
    let filename = Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("recording.cast");

    let server_hostname = server_url
        .host()
        .expect("host presence is checked in parse_server_url")
        .to_string();

    let init_request = InitiateUploadRequest {
        filename,
        byte_size,
        content_type: ASCIICAST_CONTENT_TYPE,
        changeset,
    };

    let init_response = initiate_upload_request(server_url, install_id, &init_request)
        .send()
        .await?;

    let status = init_response.status().as_u16();
    if matches!(status, 404 | 405 | 415 | 422 | 501) {
        return Ok(DirectUploadOutcome::FallbackToMultipart);
    }

    let legacy_fallback = (status == 413)
        .then(|| "The recording exceeds the server-configured size limit".to_owned());
    let init_response =
        handle_response_status(init_response, &server_hostname, legacy_fallback).await?;

    let init_body: InitiateUploadResponse = init_response.json().await?;

    if init_body.mode.as_deref() == Some("multipart") {
        return Ok(DirectUploadOutcome::FallbackToMultipart);
    }

    let Some(upload_url) = init_body
        .upload_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
    else {
        return Ok(DirectUploadOutcome::FallbackToMultipart);
    };

    upload_file_to_storage(
        upload_url,
        init_body.upload_method.as_deref(),
        init_body.headers.as_ref(),
        path,
        byte_size,
    )
    .await?;

    let complete_url = resolve_complete_url(server_url, init_body.complete_url.as_deref())?;
    let complete_request = CompleteUploadRequest {
        upload_id: init_body.upload_id.as_deref(),
        object_key: init_body.object_key.as_deref(),
        filename,
        byte_size,
        changeset,
    };

    let complete_response =
        complete_upload_request(&complete_url, install_id, &complete_request)
            .send()
            .await?;

    let complete_legacy_fallback = (complete_response.status().as_u16() == 413)
        .then(|| "The recording exceeds the server-configured size limit".to_owned());
    let complete_response = handle_response_status(
        complete_response,
        &server_hostname,
        complete_legacy_fallback,
    )
    .await?;

    let recording = complete_response.json::<RecordingResponse>().await?;

    Ok(DirectUploadOutcome::Completed(recording))
}

fn initiate_upload_request(
    server_url: &Url,
    install_id: &str,
    payload: &InitiateUploadRequest<'_>,
) -> RequestBuilder {
    let client = Client::new();
    let mut url = server_url.clone();
    url.set_path("api/v1/recordings/uploads");
    let builder = client.post(url);
    let builder = add_headers(builder, install_id);

    builder.json(payload)
}

async fn upload_file_to_storage(
    upload_url: &str,
    upload_method: Option<&str>,
    extra_headers: Option<&HashMap<String, String>>,
    path: &str,
    byte_size: u64,
) -> Result<()> {
    // Client with no overall request timeout so multi-GB uploads do not time out.
    let client = Client::builder().build()?;
    let method = upload_method
        .unwrap_or("PUT")
        .parse::<reqwest::Method>()
        .unwrap_or(reqwest::Method::PUT);

    let max_attempts = 3usize;
    for attempt in 1..=max_attempts {
        let file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("failed to open recording file {path}"))?;
        let stream = ReaderStream::with_capacity(file, UPLOAD_STREAM_BUFFER_SIZE);
        let body = Body::wrap_stream(stream);

        // CRITICAL: Do NOT call add_headers() here — never leak asciinema Basic Auth to GCS!
        let mut builder = client
            .request(method.clone(), upload_url)
            .header(header::USER_AGENT, build_user_agent())
            .header(header::CONTENT_LENGTH, byte_size);

        let mut has_content_type = false;
        if let Some(headers) = extra_headers {
            for (name, value) in headers {
                if name.eq_ignore_ascii_case("authorization")
                    || name.eq_ignore_ascii_case("content-length")
                {
                    continue;
                }
                if name.eq_ignore_ascii_case("content-type") {
                    has_content_type = true;
                }
                builder = builder.header(name.as_str(), value.as_str());
            }
        }

        if !has_content_type {
            builder = builder.header(header::CONTENT_TYPE, ASCIICAST_CONTENT_TYPE);
        }

        let response = builder
            .body(body)
            .send()
            .await
            .context("failed to upload recording data to storage")?;

        if response.status().is_success() {
            return Ok(());
        }

        let status = response.status();
        if attempt < max_attempts && matches!(status.as_u16(), 429 | 502 | 503 | 504) {
            tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt as u64))).await;
            continue;
        }

        let body_text = response.text().await.unwrap_or_default();
        if body_text.trim().is_empty() {
            bail!("storage upload failed with HTTP status {status}");
        } else {
            bail!("storage upload failed with HTTP status {status}: {body_text}");
        }
    }

    Ok(())
}

fn resolve_complete_url(server_url: &Url, complete_url: Option<&str>) -> Result<Url> {
    if let Some(raw) = complete_url.map(str::trim).filter(|s| !s.is_empty()) {
        match Url::parse(raw) {
            Ok(url) => return Ok(rebase_url_onto_server(server_url, url)),
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                return server_url
                    .join(raw)
                    .with_context(|| format!("invalid relative complete_url: {raw}"));
            }
            Err(err) => return Err(err).with_context(|| format!("invalid complete_url: {raw}")),
        }
    }

    let mut url = server_url.clone();
    url.set_path("api/v1/recordings/uploads/complete");
    Ok(url)
}

fn rebase_url_onto_server(server_url: &Url, mut target_url: Url) -> Url {
    let s_host = server_url.host_str().unwrap_or("");
    let t_host = target_url.host_str().unwrap_or("");
    let should_rebase = ((s_host == "127.0.0.1" || s_host == "localhost")
        && (t_host != s_host || target_url.port() != server_url.port()))
        || (s_host.ends_with(".run.app") && t_host.ends_with(".run.app") && s_host != t_host);

    if should_rebase {
        let _ = target_url.set_scheme(server_url.scheme());
        let _ = target_url.set_host(Some(s_host));
        let _ = target_url.set_port(server_url.port());
    }
    target_url
}

fn rebase_ws_url_onto_server(server_url: &Url, raw_ws_url: &str) -> String {
    let Ok(mut ws_url) = Url::parse(raw_ws_url) else {
        return raw_ws_url.to_owned();
    };
    let s_host = server_url.host_str().unwrap_or("");
    let t_host = ws_url.host_str().unwrap_or("");
    let should_rebase = ((s_host == "127.0.0.1" || s_host == "localhost")
        && (t_host != s_host || ws_url.port() != server_url.port()))
        || (s_host.ends_with(".run.app") && t_host.ends_with(".run.app") && s_host != t_host);

    if should_rebase {
        let ws_scheme = if server_url.scheme() == "https" {
            "wss"
        } else {
            "ws"
        };
        let _ = ws_url.set_scheme(ws_scheme);
        let _ = ws_url.set_host(Some(s_host));
        let _ = ws_url.set_port(server_url.port());
        return ws_url.to_string();
    }
    raw_ws_url.to_owned()
}

fn complete_upload_request(
    complete_url: &Url,
    install_id: &str,
    payload: &CompleteUploadRequest<'_>,
) -> RequestBuilder {
    let client = Client::new();
    let builder = client.post(complete_url.clone());
    let builder = add_headers(builder, install_id);

    builder.json(payload)
}

async fn create_recording_request(
    server_url: &Url,
    install_id: String,
    path: &str,
    changeset: RecordingChangeset,
) -> Result<RequestBuilder> {
    let client = Client::new();
    let mut url = server_url.clone();
    url.set_path("api/v1/recordings");
    let form = Form::new().file("file", path).await?;
    let form = add_recording_changeset_fields(form, changeset);
    let builder = client.post(url).multipart(form);

    Ok(add_headers(builder, &install_id))
}

fn add_recording_changeset_fields(mut form: Form, changeset: RecordingChangeset) -> Form {
    if let Some(Some(title)) = changeset.title {
        form = form.text("title", title);
    }

    if let Some(Some(description)) = changeset.description {
        form = form.text("description", description);
    }

    if let Some(visibility) = changeset.visibility {
        let visibility = match visibility {
            Visibility::Public => "public",
            Visibility::Unlisted => "unlisted",
            Visibility::Private => "private",
        };

        form = form.text("visibility", visibility);
    }

    if let Some(Some(audio_url)) = changeset.audio_url {
        form = form.text("audio_url", audio_url);
    }

    form
}

pub async fn list_user_streams(prefix: &str, config: &mut Config) -> Result<Vec<StreamResponse>> {
    let server_url = config.get_server_url()?;
    let install_id = config.get_install_id()?;

    let response = list_user_streams_request(&server_url, prefix, &install_id)
        .send()
        .await
        .context("cannot obtain stream producer endpoint - is the server down?")?;

    parse_stream_response(response, &server_url).await
}

fn list_user_streams_request(server_url: &Url, prefix: &str, install_id: &str) -> RequestBuilder {
    let client = Client::new();
    let mut url = server_url.clone();
    url.set_path("api/v1/user/streams");
    url.set_query(Some(&format!("prefix={prefix}&limit=10")));

    add_headers(client.get(url), install_id)
}

pub async fn create_stream(
    changeset: StreamChangeset,
    config: &mut Config,
) -> Result<StreamResponse> {
    let server_url = config.get_server_url()?;
    let install_id = config.get_install_id()?;

    let response = create_stream_request(&server_url, &install_id, changeset)
        .send()
        .await
        .context("cannot obtain stream producer endpoint - is the server down?")?;

    let mut stream: StreamResponse = parse_stream_response(response, &server_url).await?;
    stream.ws_producer_url = rebase_ws_url_onto_server(&server_url, &stream.ws_producer_url);
    if let Ok(parsed_url) = Url::parse(&stream.url) {
        stream.url = rebase_url_onto_server(&server_url, parsed_url).to_string();
    }
    Ok(stream)
}

fn create_stream_request(
    server_url: &Url,
    install_id: &str,
    changeset: StreamChangeset,
) -> RequestBuilder {
    let client = Client::new();
    let mut url = server_url.clone();
    url.set_path("api/v1/streams");
    let builder = client.post(url);
    let builder = add_headers(builder, install_id);

    builder.json(&changeset)
}

pub async fn update_stream(
    stream_id: u64,
    changeset: StreamChangeset,
    config: &mut Config,
) -> Result<StreamResponse> {
    let server_url = config.get_server_url()?;
    let install_id = config.get_install_id()?;

    let response = update_stream_request(&server_url, &install_id, stream_id, changeset)
        .send()
        .await
        .context("cannot obtain stream producer endpoint - is the server down?")?;

    let mut stream: StreamResponse = parse_stream_response(response, &server_url).await?;
    stream.ws_producer_url = rebase_ws_url_onto_server(&server_url, &stream.ws_producer_url);
    if let Ok(parsed_url) = Url::parse(&stream.url) {
        stream.url = rebase_url_onto_server(&server_url, parsed_url).to_string();
    }
    Ok(stream)
}

fn update_stream_request(
    server_url: &Url,
    install_id: &str,
    stream_id: u64,
    changeset: StreamChangeset,
) -> RequestBuilder {
    let client = Client::new();
    let mut url = server_url.clone();
    url.set_path(&format!("api/v1/streams/{stream_id}"));
    let builder = client.patch(url);
    let builder = add_headers(builder, install_id);

    builder.json(&changeset)
}

async fn parse_stream_response<T: DeserializeOwned>(
    response: Response,
    server_url: &Url,
) -> Result<T> {
    let server_hostname = server_url
        .host()
        .expect("host presence is checked in parse_server_url")
        .to_string();

    let legacy_fallback = match response.status().as_u16() {
        404 | 422 => Some(format!("{server_hostname} doesn't support streaming")),
        _ => None,
    };

    let response = handle_response_status(response, &server_hostname, legacy_fallback).await?;

    response.json::<T>().await.map_err(|e| e.into())
}

async fn handle_response_status(
    response: Response,
    server_hostname: &str,
    legacy_fallback: Option<String>,
) -> Result<Response> {
    let status_error = match response.error_for_status_ref() {
        Ok(_) => return Ok(response),
        Err(error) => error,
    };

    let message = match response.bytes().await {
        Ok(body) => parse_error_response(&body)
            .and_then(|response| render_error_response(response, server_hostname)),
        Err(_) => None,
    };

    if let Some(message) = message.or(legacy_fallback) {
        bail!(message);
    }

    Err(status_error.into())
}

fn parse_error_response(body: &[u8]) -> Option<ErrorResponse> {
    serde_json::from_slice(body).ok()
}

fn render_error_response(response: ErrorResponse, server_hostname: &str) -> Option<String> {
    let guidance = match response.error_type.as_deref() {
        Some("account_required") => Some(format!(
            "Run `asciinema auth` to link this CLI to your {server_hostname} account."
        )),

        Some("upload_limit_reached") => {
            Some("Run `asciinema auth` and follow the link to upload more.".to_owned())
        }

        _ => None,
    };

    let mut message = format_error_response(response)?;

    if let Some(guidance) = guidance {
        message.push_str("\n\n");
        message.push_str(&guidance);
    }

    Some(message)
}

fn format_error_response(response: ErrorResponse) -> Option<String> {
    let mut message = response
        .message
        .filter(|message| !message.trim().is_empty())?;

    if response.error_type.as_deref() != Some("validation_failed") {
        return Some(message);
    }

    if let Some(serde_json::Value::Array(details)) = response.details {
        let mut has_details = false;

        for value in details {
            let Ok(detail) = serde_json::from_value::<ErrorDetail>(value) else {
                continue;
            };

            if detail.message.trim().is_empty() {
                continue;
            }

            if !has_details {
                if !message.ends_with(':') {
                    message.push(':');
                }

                has_details = true;
            }

            match detail.field.as_deref().map(str::trim) {
                Some(field) if !field.is_empty() && field != "." => {
                    message.push_str(&format!("\n  {field}: {}", detail.message));
                }
                _ => message.push_str(&format!("\n  {}", detail.message)),
            }
        }
    }

    Some(message)
}

fn add_headers(builder: RequestBuilder, install_id: &str) -> RequestBuilder {
    builder
        .basic_auth(get_username(), Some(install_id))
        .header(header::USER_AGENT, build_user_agent())
        .header(header::ACCEPT, "application/json")
}

fn get_username() -> String {
    env::var("USER").unwrap_or("".to_owned())
}

pub fn build_user_agent() -> String {
    let ua = concat!(
        "asciinema/",
        env!("CARGO_PKG_VERSION"),
        " target/",
        env!("TARGET")
    );

    ua.to_owned()
}

#[cfg(test)]
mod tests {
    use axum::http::{Response as HttpResponse, StatusCode};
    use tokio::runtime::Runtime;

    use super::{
        format_error_response, handle_response_status, parse_error_response, render_error_response,
    };

    const SERVER_HOSTNAME: &str = "example.com";

    fn response(status: StatusCode, body: &'static str) -> reqwest::Response {
        HttpResponse::builder()
            .status(status)
            .body(body)
            .unwrap()
            .into()
    }

    fn parse_error_message(body: &[u8]) -> Option<String> {
        parse_error_response(body).and_then(format_error_response)
    }

    fn render_error_message(body: &[u8]) -> Option<String> {
        parse_error_response(body)
            .and_then(|response| render_error_response(response, SERVER_HOSTNAME))
    }

    #[test]
    fn augments_account_required_error() {
        let body = br#"{
            "type": "account_required",
            "message": "This action requires an account"
        }"#;

        assert_eq!(
            render_error_message(body),
            Some(
                "This action requires an account\n\n\
                 Run `asciinema auth` to link this CLI to your example.com account."
                    .to_owned()
            )
        );
    }

    #[test]
    fn augments_upload_limit_reached_error() {
        let body = br#"{
            "type": "upload_limit_reached",
            "message": "Anonymous upload limit reached"
        }"#;

        assert_eq!(
            render_error_message(body),
            Some(
                "Anonymous upload limit reached\n\n\
                 Run `asciinema auth` and follow the link to upload more."
                    .to_owned()
            )
        );

        let error = Runtime::new()
            .unwrap()
            .block_on(handle_response_status(
                response(StatusCode::FORBIDDEN, std::str::from_utf8(body).unwrap()),
                SERVER_HOSTNAME,
                None,
            ))
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Anonymous upload limit reached\n\n\
             Run `asciinema auth` and follow the link to upload more."
        );
    }

    #[test]
    fn leaves_unauthenticated_error_verbatim() {
        let body = br#"{
            "type": "unauthenticated",
            "message": "Installation ID has been revoked"
        }"#;

        assert_eq!(
            render_error_message(body),
            Some("Installation ID has been revoked".to_owned())
        );
    }

    #[test]
    fn leaves_unknown_error_type_verbatim() {
        let body = br#"{
            "type": "future_error",
            "message": "A future server error"
        }"#;

        assert_eq!(
            render_error_message(body),
            Some("A future server error".to_owned())
        );
    }

    #[test]
    fn formats_validation_error_details() {
        let body = br#"{
            "type": "validation_failed",
            "message": "Validation failed",
            "details": [
                {
                    "field": "audio_url",
                    "message": "has invalid format"
                },
                {
                    "field": "audio_url",
                    "message": "should be at most 255 character(s)"
                }
            ]
        }"#;

        assert_eq!(
            render_error_message(body),
            Some(
                "Validation failed:\n  audio_url: has invalid format\n  \
                 audio_url: should be at most 255 character(s)"
                    .to_owned()
            )
        );
    }

    #[test]
    fn ignores_invalid_validation_error_details() {
        for body in [
            br#"{
                "type": "validation_failed",
                "message": "Validation failed"
            }"#
            .as_slice(),
            br#"{
                "type": "validation_failed",
                "message": "Validation failed",
                "details": "invalid"
            }"#
            .as_slice(),
            br#"{
                "type": "validation_failed",
                "message": "Validation failed",
                "details": [
                    {"field": "idle_time_limit"},
                    {"field": "", "message": ""}
                ]
            }"#
            .as_slice(),
        ] {
            assert_eq!(
                parse_error_message(body),
                Some("Validation failed".to_owned())
            );
        }
    }

    #[test]
    fn formats_fieldless_validation_error_details() {
        let body = br#"{
            "type": "validation_failed",
            "message": "Validation failed",
            "details": [
                {"message": "recording metadata is invalid"},
                {"field": "", "message": "recording data is invalid"},
                {"field": "  ", "message": "recording options are invalid"},
                {"field": ".", "message": "recording is invalid"}
            ]
        }"#;

        assert_eq!(
            parse_error_message(body),
            Some(
                "Validation failed:\n  recording metadata is invalid\n  \
                 recording data is invalid\n  recording options are invalid\n  recording is invalid"
                    .to_owned()
            )
        );
    }

    #[test]
    fn ignores_error_body_without_message() {
        assert_eq!(
            parse_error_message(br#"{"type":"upload_limit_reached"}"#),
            None
        );
        assert_eq!(parse_error_message(br#"{"message":""}"#), None);
    }

    #[test]
    fn falls_back_to_status_error_for_invalid_or_empty_body() {
        assert_eq!(parse_error_message(b"not JSON"), None);
        assert_eq!(parse_error_message(b""), None);

        for body in ["not JSON", ""] {
            let error = Runtime::new()
                .unwrap()
                .block_on(handle_response_status(
                    response(StatusCode::UNPROCESSABLE_ENTITY, body),
                    SERVER_HOSTNAME,
                    Some("The server doesn't support streaming".to_owned()),
                ))
                .unwrap_err();

            assert_eq!(error.to_string(), "The server doesn't support streaming");
        }

        for body in ["not JSON", ""] {
            let error = Runtime::new()
                .unwrap()
                .block_on(handle_response_status(
                    response(StatusCode::FORBIDDEN, body),
                    SERVER_HOSTNAME,
                    None,
                ))
                .unwrap_err();

            assert!(error.to_string().contains("403 Forbidden"));
        }
    }

    use std::sync::{Arc, Mutex};
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{header, HeaderMap};
    use axum::response::IntoResponse;
    use axum::routing::{post, put};
    use axum::Router;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    use super::{create_recording, RecordingChangeset, Visibility};
    use crate::config::Config;

    #[derive(Debug, Clone)]
    struct RecordedRequest {
        headers: HeaderMap,
        body: Vec<u8>,
    }

    #[derive(Default)]
    struct MockUploadState {
        initiate_requests: Vec<RecordedRequest>,
        gcs_put_requests: Vec<RecordedRequest>,
        complete_requests: Vec<RecordedRequest>,
        multipart_requests: Vec<RecordedRequest>,
        initiate_status: Option<StatusCode>,
        initiate_body: Option<String>,
        complete_url_override: Option<String>,
        gcs_status: Option<StatusCode>,
        complete_status: Option<StatusCode>,
    }

    fn spawn_mock_upload_server(
        rt: &Runtime,
        initial_state: MockUploadState,
    ) -> (String, Arc<Mutex<MockUploadState>>, oneshot::Sender<()>) {
        let state = Arc::new(Mutex::new(initial_state));
        let state_clone = Arc::clone(&state);

        let (listener, addr) = rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            (listener, addr)
        });

        let base_url = format!("http://{addr}");
        let base_url_for_routes = base_url.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let app = Router::new()
            .route(
                "/api/v1/recordings/uploads",
                post({
                    let base_url = base_url_for_routes.clone();
                    move |State(st): State<Arc<Mutex<MockUploadState>>>,
                          headers: HeaderMap,
                          body: Bytes| async move {
                        let mut guard = st.lock().unwrap();
                        guard.initiate_requests.push(RecordedRequest {
                            headers,
                            body: body.to_vec(),
                        });

                        let status = guard.initiate_status.unwrap_or(StatusCode::OK);
                        if let Some(custom_body) = &guard.initiate_body {
                            return (
                                status,
                                [(header::CONTENT_TYPE, "application/json")],
                                custom_body.clone(),
                            )
                                .into_response();
                        }

                        let complete_url = guard.complete_url_override.clone().unwrap_or_else(|| {
                            format!("{base_url}/api/v1/recordings/uploads/complete")
                        });

                        let resp_json = serde_json::json!({
                            "mode": "direct",
                            "upload_id": "upl-test-123",
                            "object_key": "tmp/direct-uploads/obj-test-123.cast",
                            "upload_url": format!("{base_url}/gcs/upload/obj-test-123.cast?sig=abc"),
                            "upload_method": "PUT",
                            "upload_type": "resumable",
                            "headers": {
                                "Content-Type": "application/x-asciicast",
                                "Authorization": "Bearer should-be-filtered-out",
                                "x-goog-meta-source": "asciinema-cli-test"
                            },
                            "complete_url": complete_url
                        });

                        (
                            status,
                            [(header::CONTENT_TYPE, "application/json")],
                            resp_json.to_string(),
                        )
                            .into_response()
                    }
                }),
            )
            .route(
                "/gcs/upload/{*key}",
                put(
                    |State(st): State<Arc<Mutex<MockUploadState>>>,
                     headers: HeaderMap,
                     body: Bytes| async move {
                        let mut guard = st.lock().unwrap();
                        guard.gcs_put_requests.push(RecordedRequest {
                            headers,
                            body: body.to_vec(),
                        });
                        let status = guard.gcs_status.unwrap_or(StatusCode::OK);
                        (status, "").into_response()
                    },
                ),
            )
            .route(
                "/api/v1/recordings/uploads/complete",
                post(
                    |State(st): State<Arc<Mutex<MockUploadState>>>,
                     headers: HeaderMap,
                     body: Bytes| async move {
                        let mut guard = st.lock().unwrap();
                        guard.complete_requests.push(RecordedRequest {
                            headers,
                            body: body.to_vec(),
                        });
                        let status = guard.complete_status.unwrap_or(StatusCode::CREATED);
                        let resp_json = serde_json::json!({
                            "id": 101,
                            "url": "https://example.com/a/direct-rec-101",
                            "file_url": "https://example.com/a/direct-rec-101.cast",
                            "message": "View the direct recording at:\n\n    https://example.com/a/direct-rec-101\n"
                        });
                        (
                            status,
                            [(header::CONTENT_TYPE, "application/json")],
                            resp_json.to_string(),
                        )
                            .into_response()
                    },
                ),
            )
            .route(
                "/api/v1/recordings",
                post(
                    |State(st): State<Arc<Mutex<MockUploadState>>>,
                     headers: HeaderMap,
                     body: Bytes| async move {
                        let mut guard = st.lock().unwrap();
                        guard.multipart_requests.push(RecordedRequest {
                            headers,
                            body: body.to_vec(),
                        });
                        let resp_json = serde_json::json!({
                            "id": 202,
                            "url": "https://example.com/a/multipart-rec-202",
                            "message": "View the multipart recording at:\n\n    https://example.com/a/multipart-rec-202\n"
                        });
                        (
                            StatusCode::CREATED,
                            [(header::CONTENT_TYPE, "application/json")],
                            resp_json.to_string(),
                        )
                            .into_response()
                    },
                ),
            )
            .with_state(state_clone);

        rt.spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        (base_url, state, shutdown_tx)
    }

    #[test]
    fn direct_to_gcs_upload_handshake_and_streaming_put_without_auth_leak() {
        let rt = Runtime::new().unwrap();
        let (base_url, state, _shutdown) =
            spawn_mock_upload_server(&rt, MockUploadState::default());

        let mut config = Config::new_for_test(&base_url, "install-id-secret-999");
        let fixture_path = "tests/casts/minimal-v3.cast";
        let expected_bytes = std::fs::read(fixture_path).unwrap();

        let changeset = RecordingChangeset {
            title: Some(Some("My Direct Upload".to_owned())),
            description: Some(Some("Recorded for M3".to_owned())),
            visibility: Some(Visibility::Unlisted),
            audio_url: None,
        };

        let result = rt
            .block_on(create_recording(fixture_path, changeset, &mut config))
            .unwrap();

        assert_eq!(result.url, "https://example.com/a/direct-rec-101");

        let guard = state.lock().unwrap();
        assert_eq!(guard.initiate_requests.len(), 1);
        assert_eq!(guard.gcs_put_requests.len(), 1);
        assert_eq!(guard.complete_requests.len(), 1);
        assert_eq!(guard.multipart_requests.len(), 0);

        // Verify Step 1 (Initiate) request
        let init_req = &guard.initiate_requests[0];
        assert!(init_req.headers.get(header::AUTHORIZATION).is_some());
        let init_json: serde_json::Value = serde_json::from_slice(&init_req.body).unwrap();
        assert_eq!(init_json["filename"], "minimal-v3.cast");
        assert_eq!(init_json["byte_size"], expected_bytes.len() as u64);
        assert_eq!(init_json["content_type"], "application/x-asciicast");
        assert_eq!(init_json["title"], "My Direct Upload");
        assert_eq!(init_json["description"], "Recorded for M3");
        assert_eq!(init_json["visibility"], "unlisted");

        // Verify Step 2 (GCS PUT) request: exact bytes, Content-Length, Content-Type, custom headers, NO Authorization!
        let gcs_req = &guard.gcs_put_requests[0];
        assert_eq!(gcs_req.body, expected_bytes);
        assert_eq!(
            gcs_req
                .headers
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok()),
            Some(expected_bytes.len().to_string().as_str())
        );
        assert_eq!(
            gcs_req
                .headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/x-asciicast")
        );
        assert_eq!(
            gcs_req
                .headers
                .get("x-goog-meta-source")
                .and_then(|v| v.to_str().ok()),
            Some("asciinema-cli-test")
        );
        assert!(
            gcs_req.headers.get(header::AUTHORIZATION).is_none(),
            "Authorization header MUST NOT be sent to GCS signed/resumable upload URL"
        );

        // Verify Step 3 (Complete) request
        let comp_req = &guard.complete_requests[0];
        assert!(comp_req.headers.get(header::AUTHORIZATION).is_some());
        let comp_json: serde_json::Value = serde_json::from_slice(&comp_req.body).unwrap();
        assert_eq!(comp_json["upload_id"], "upl-test-123");
        assert_eq!(
            comp_json["object_key"],
            "tmp/direct-uploads/obj-test-123.cast"
        );
        assert_eq!(comp_json["filename"], "minimal-v3.cast");
        assert_eq!(comp_json["byte_size"], expected_bytes.len() as u64);
        assert_eq!(comp_json["title"], "My Direct Upload");
    }

    #[test]
    fn direct_upload_resolves_relative_and_default_complete_url() {
        let rt = Runtime::new().unwrap();

        for complete_override in ["/api/v1/recordings/uploads/complete", ""] {
            let initial = MockUploadState {
                complete_url_override: Some(complete_override.to_owned()),
                ..Default::default()
            };
            let (base_url, state, _shutdown) = spawn_mock_upload_server(&rt, initial);
            let mut config = Config::new_for_test(&base_url, "install-id-rel");

            let result = rt
                .block_on(create_recording(
                    "tests/casts/minimal-v3.cast",
                    RecordingChangeset::default(),
                    &mut config,
                ))
                .unwrap();

            assert_eq!(result.url, "https://example.com/a/direct-rec-101");
            let guard = state.lock().unwrap();
            assert_eq!(guard.complete_requests.len(), 1);
        }
    }

    #[test]
    fn falls_back_to_multipart_on_legacy_or_local_filestore_responses() {
        let rt = Runtime::new().unwrap();

        let fallback_cases: Vec<(StatusCode, Option<&str>)> = vec![
            (StatusCode::NOT_FOUND, Some(r#"{"error": "not_found"}"#)),
            (StatusCode::METHOD_NOT_ALLOWED, Some("")),
            (StatusCode::UNSUPPORTED_MEDIA_TYPE, Some("")),
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(r#"{"message": "direct upload not supported"}"#),
            ),
            (StatusCode::NOT_IMPLEMENTED, Some("")),
            (StatusCode::OK, Some(r#"{"mode": "multipart"}"#)),
            (StatusCode::OK, Some(r#"{"mode": "direct"}"#)),
        ];

        for (status, body) in fallback_cases {
            let initial = MockUploadState {
                initiate_status: Some(status),
                initiate_body: body.map(str::to_owned),
                ..Default::default()
            };
            let (base_url, state, _shutdown) = spawn_mock_upload_server(&rt, initial);
            let mut config = Config::new_for_test(&base_url, "install-id-fallback");

            let changeset = RecordingChangeset {
                title: Some(Some("Fallback Title".to_owned())),
                visibility: Some(Visibility::Public),
                ..Default::default()
            };

            let result = rt
                .block_on(create_recording(
                    "tests/casts/minimal-v2.cast",
                    changeset,
                    &mut config,
                ))
                .unwrap();

            assert_eq!(
                result.url, "https://example.com/a/multipart-rec-202",
                "failed fallback for status {status}"
            );

            let guard = state.lock().unwrap();
            assert_eq!(guard.initiate_requests.len(), 1);
            assert_eq!(guard.gcs_put_requests.len(), 0);
            assert_eq!(guard.complete_requests.len(), 0);
            assert_eq!(guard.multipart_requests.len(), 1);

            let mp_req = &guard.multipart_requests[0];
            assert!(mp_req.headers.get(header::AUTHORIZATION).is_some());
            let mp_body = String::from_utf8_lossy(&mp_req.body);
            assert!(mp_body.contains("Fallback Title"));
            assert!(mp_body.contains("public"));
        }
    }

    #[test]
    fn propagates_error_without_multipart_fallback_on_401_403_413_and_500() {
        let rt = Runtime::new().unwrap();

        let error_cases = vec![
            (
                StatusCode::UNAUTHORIZED,
                r#"{"type": "unauthenticated", "message": "Installation ID has been revoked"}"#,
                "Installation ID has been revoked",
            ),
            (
                StatusCode::FORBIDDEN,
                r#"{"type": "account_required", "message": "This action requires an account"}"#,
                "This action requires an account",
            ),
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                "",
                "The recording exceeds the server-configured size limit",
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"message": "Internal database error"}"#,
                "Internal database error",
            ),
        ];

        for (status, body, expected_msg) in error_cases {
            let initial = MockUploadState {
                initiate_status: Some(status),
                initiate_body: Some(body.to_owned()),
                ..Default::default()
            };
            let (base_url, state, _shutdown) = spawn_mock_upload_server(&rt, initial);
            let mut config = Config::new_for_test(&base_url, "install-id-err");

            let err = rt
                .block_on(create_recording(
                    "tests/casts/minimal-v2.cast",
                    RecordingChangeset::default(),
                    &mut config,
                ))
                .unwrap_err();

            assert!(
                err.to_string().contains(expected_msg),
                "expected '{expected_msg}' in '{err}'"
            );

            let guard = state.lock().unwrap();
            assert_eq!(guard.initiate_requests.len(), 1);
            assert_eq!(guard.gcs_put_requests.len(), 0);
            assert_eq!(guard.complete_requests.len(), 0);
            assert_eq!(
                guard.multipart_requests.len(),
                0,
                "must NOT fall back to multipart on {status}"
            );
        }
    }
}
