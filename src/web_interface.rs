use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

use build_html::Html;
use build_html::HtmlContainer;
use build_html::Table;
use build_html::{Container, ContainerType, HtmlPage};
use hyper::body::Incoming;
use hyper::{
    Request, Response, StatusCode,
    header::{HeaderValue, SERVER},
};
use log::{debug, error, trace};
use time::OffsetDateTime;
use time::format_description::FormatItem;
use time::macros::format_description;

use crate::APP_VERSION;
use crate::HumanFmt;
use crate::ProxyCacheBody;
use crate::RUNTIMEDETAILS;
use crate::global_config;
use crate::{APP_NAME, LOGSTORE, database::Database, error::ProxyCacheError, full, quick_response};

const WEBUI_DATE_FORMAT: &[FormatItem<'_>] =
    format_description!("[day] [month repr:short] [year] [hour]:[minute]:[second]");

#[must_use]
pub(crate) async fn serve_web_interface(
    req: Request<Incoming>,
    database: Database,
) -> Response<ProxyCacheBody> {
    let location = req.uri().path();
    debug!("Requested local web interface resource `{location}`");

    match location {
        "/" => serve_root(database).await,
        "/logs" => serve_logs(),
        _ => {
            debug!("Invalid local web interface request: {req:?}");

            quick_response(
                StatusCode::BAD_REQUEST,
                "Local interface resource not available",
            )
        }
    }
}

/// Returns a tuple of the number of files and their total size in bytes on success.
async fn flat_directory_size(path: &Path) -> Result<(usize, u64), tokio::io::Error> {
    let mut dir = tokio::fs::read_dir(path).await?;

    let mut total_files = 0;
    let mut total_size = 0;

    while let Some(entry) = dir.next_entry().await? {
        let mdata = entry.metadata().await?;
        total_size += mdata.len();
        #[expect(clippy::case_sensitive_file_extension_comparisons)]
        if mdata.is_file()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|s| s.ends_with(".deb"))
        {
            total_files += 1;
        }
    }

    Ok((total_files, total_size))
}

async fn build_mirror_table(database: &Database) -> Result<Table, ProxyCacheError> {
    let mut html_table_mirrors = Table::new().with_header_row(&[
        "Mirror",
        "Last Seen",
        "First Seen",
        "Last Cleanup",
        "Total Download Size",
        "Total Delivery Size",
        "Cache Rate",
        "Disk Space",
        "File Count",
    ]);

    let mut mirrors = match database.get_mirrors_with_stats().await {
        Ok(m) => m,
        Err(err) => {
            error!("Failed to query mirrors:  {err}");
            return Err(ProxyCacheError::Sqlx(err));
        }
    };
    mirrors.sort_unstable_by_key(|mirror| -mirror.last_seen);

    let cache_path = &global_config().cache_directory;

    for mirror in mirrors {
        let last_seen = time::OffsetDateTime::from_unix_timestamp(mirror.last_seen)
            .expect("timestamp should be valid");
        let last_seen_fmt = last_seen
            .format(WEBUI_DATE_FORMAT)
            .expect("timestamp should be formattable");

        let first_seen = time::OffsetDateTime::from_unix_timestamp(mirror.first_seen)
            .expect("timestamp should be valid");
        let first_seen_fmt = first_seen
            .format(WEBUI_DATE_FORMAT)
            .expect("timestamp should be formattable");

        let last_cleanup_fmt = if mirror.last_cleanup == 0 {
            "never".to_string()
        } else {
            let last_cleanup = time::OffsetDateTime::from_unix_timestamp(mirror.last_cleanup)
                .expect("timestamp should be valid");
            last_cleanup
                .format(WEBUI_DATE_FORMAT)
                .expect("timestamp should be formattable")
        };

        let total_download_size_fmt = format!(
            "{}",
            HumanFmt::Size(
                u64::try_from(mirror.total_download_size)
                    .expect("Database should never store negative size")
            )
        );

        let total_delivery_size_fmt = format!(
            "{}",
            HumanFmt::Size(
                u64::try_from(mirror.total_delivery_size)
                    .expect("Database should never store negative size")
            )
        );

        let mirror_path: PathBuf = [cache_path, Path::new(&mirror.host), Path::new(&mirror.path)]
            .iter()
            .collect();
        let (file_count_fmt, dir_size_fmt) = if mirror_path.exists() {
            match flat_directory_size(&mirror_path).await {
                Ok((count, size)) => (format!("{count}"), format!("{}", HumanFmt::Size(size))),
                Err(err) => {
                    error!(
                        "Failed to gather size of directory `{}`:  {err}",
                        mirror_path.display()
                    );
                    ("N/A".to_string(), "N/A".to_string())
                }
            }
        } else {
            ("N/A".to_string(), "N/A".to_string())
        };

        #[expect(clippy::cast_precision_loss)]
        let rate_fmt = if mirror.total_download_size == 0 {
            "N/A".to_string()
        } else {
            format!(
                "{:.2}",
                mirror.total_delivery_size as f32 / mirror.total_download_size as f32
            )
        };

        html_table_mirrors.add_body_row(&[
            format!("{}/{}", mirror.host, mirror.path),
            last_seen_fmt,
            first_seen_fmt,
            last_cleanup_fmt,
            total_download_size_fmt,
            total_delivery_size_fmt,
            rate_fmt,
            dir_size_fmt,
            file_count_fmt,
        ]);
    }

    Ok(html_table_mirrors)
}

async fn build_origin_table(database: &Database) -> Result<Table, ProxyCacheError> {
    let mut html_table_origins = Table::new().with_header_row(&[
        "Mirror",
        "Distribution",
        "Component",
        "Architecture",
        "Last Seen",
    ]);

    let mut origins = match database.get_origins().await {
        Ok(o) => o,
        Err(err) => {
            error!("Failed to query origins:  {err}");
            return Err(ProxyCacheError::Sqlx(err));
        }
    };
    origins.sort_unstable_by_key(|origin| -origin.last_seen);

    for origin in origins {
        let last_seen = time::OffsetDateTime::from_unix_timestamp(origin.last_seen)
            .expect("timestamp should be valid");
        let last_seen_fmt = last_seen
            .format(WEBUI_DATE_FORMAT)
            .expect("timestamp should be formattable");

        html_table_origins.add_body_row(&[
            format!("{}/{}", origin.host, origin.mirror_path),
            origin.distribution,
            origin.component,
            origin.architecture,
            last_seen_fmt,
        ]);
    }

    Ok(html_table_origins)
}

async fn build_client_table(database: &Database) -> Result<Table, ProxyCacheError> {
    let mut html_table_clients = Table::new().with_header_row(&["IP"]);

    let clients = match database.get_clients().await {
        Ok(o) => o,
        Err(err) => {
            error!("Failed to query clients:  {err}");
            return Err(ProxyCacheError::Sqlx(err));
        }
    };

    for client in clients {
        html_table_clients.add_body_row([client]);
    }

    Ok(html_table_clients)
}

#[must_use]
async fn serve_root(database: Database) -> Response<ProxyCacheBody> {
    let start = Instant::now();

    let Ok(html_table_mirrors) = build_mirror_table(&database).await else {
        return quick_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Local Webinterface failure",
        );
    };

    let Ok(html_table_origins) = build_origin_table(&database).await else {
        return quick_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Local Webinterface failure",
        );
    };

    let Ok(html_table_clients) = build_client_table(&database).await else {
        return quick_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Local Webinterface failure",
        );
    };

    let rd = RUNTIMEDETAILS.get().expect("Should be set");
    let database_size_fmt = match tokio::fs::metadata(&rd.config.database_path).await {
        Ok(data) => format!("{}", HumanFmt::Size(data.len())),
        Err(err) => {
            error!(
                "Failed to access database file `{}`:  {err}",
                rd.config.database_path.display()
            );
            "N/A".to_string()
        }
    };

    let html: String = HtmlPage::new()
        .with_title("apt-cacher-rs web interface")
        .with_header(1, "Statistics")
        .with_container(
            Container::new(ContainerType::Div)
                .with_header(2, "Program Details")
                .with_paragraph(format!(
                    "Version: {}<br>Start Time: {}<br>Current Time: {}<br>Bind Address: {}<br>Bind Port: {}<br>Database Size: {}",
                    APP_VERSION,
                    rd.start_time
                        .format(WEBUI_DATE_FORMAT)
                        .expect("timestamp should be formattable"),
                    OffsetDateTime::now_utc()
                        .format(WEBUI_DATE_FORMAT)
                        .expect("timestamp should be formattable"),
                    rd.config.bind_addr,
                    rd.config.bind_port,
                    database_size_fmt,
                )).with_link("/logs", "Logs"),
        )
        .with_container(
            Container::new(ContainerType::Div)
                .with_header_attr(2, "Mirrors", [("id", "mirrors-head")])
                .with_paragraph("List of proxied mirrors:")
                .with_table(html_table_mirrors),
        )
        .with_container(
            Container::new(ContainerType::Div)
                .with_header_attr(2, "Origins", [("id", "origins-head")])
                .with_paragraph("List of proxied origins:")
                .with_table(html_table_origins),
        )
        .with_container(
            Container::new(ContainerType::Div)
                .with_header_attr(2, "Clients", [("id", "clients-head")])
                .with_paragraph("List of clients:")
                .with_table(html_table_clients),
        )
        .with_container(
            Container::new(ContainerType::Footer).with_paragraph(format!("<hr>All dates are in UTC.   --   Generated in {}.", HumanFmt::Time(start.elapsed()))),
        )
        .to_html_string();

    let boxed_body = ProxyCacheBody::Boxed(full(html));

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(SERVER, HeaderValue::from_static(APP_NAME))
        .body(boxed_body)
        .expect("HTTP response is valid");

    trace!("Local web interface response: {response:?}");

    response
}

#[must_use]
fn serve_logs() -> Response<ProxyCacheBody> {
    let mut buf = Vec::with_capacity(8192);

    let ls = LOGSTORE.get().expect("Should be set");
    for entry in ls.entries().iter() {
        buf.extend_from_slice(entry.as_bytes());
        buf.push(b'\n');
    }

    let boxed_body = ProxyCacheBody::Boxed(full(buf));

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(SERVER, HeaderValue::from_static(APP_NAME))
        .body(boxed_body)
        .expect("HTTP response is valid");

    trace!("Local web interface response: {response:?}");

    response
}
