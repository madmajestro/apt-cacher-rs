use std::net::IpAddr;

use coarsetime::Duration;

use crate::rate_checked_body::DownloadRateError;
use crate::{ChannelBodyError, ContentLength};
use crate::{deb_mirror::Mirror, humanfmt::HumanFmt};

#[derive(Copy, Clone, Debug)]
pub(crate) struct ClientDownloadRate {
    pub(crate) download_rate_err: DownloadRateError,
    pub(crate) client_ip: IpAddr,
}

#[derive(Clone, Debug)]
pub(crate) struct MirrorDownloadRate {
    pub(crate) download_rate_err: DownloadRateError,
    pub(crate) mirror: Mirror,
    pub(crate) debname: String,
    pub(crate) client_ip: IpAddr,
}

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum ProxyCacheError {
    Io(std::io::Error),
    Hyper(hyper::Error),
    HyperUtil(hyper_util::client::legacy::Error),
    Sqlx(sqlx::Error),
    SystemTime(std::time::SystemTimeError),
    Utf8(std::string::FromUtf8Error),
    ClientDownloadRate(ClientDownloadRate),
    MirrorDownloadRate(MirrorDownloadRate),
    Memfd(memfd::Error),
    ContentTooLarge(ContentLength, u64),
}

impl std::fmt::Display for ProxyCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Hyper(e) => e.fmt(f),
            Self::HyperUtil(e) => e.fmt(f),
            Self::Sqlx(e) => e.fmt(f),
            Self::SystemTime(e) => e.fmt(f),
            Self::Utf8(e) => e.fmt(f),
            Self::ClientDownloadRate(ClientDownloadRate {
                download_rate_err,
                client_ip,
            }) => {
                write!(
                    f,
                    "Timeout occurred for client {} after a download rate of {} for the last {} seconds",
                    client_ip.to_canonical(),
                    HumanFmt::Rate(
                        download_rate_err.download_size as u64,
                        Duration::from_secs(download_rate_err.timeframe.get() as u64)
                    ),
                    download_rate_err.timeframe,
                )
            }
            Self::MirrorDownloadRate(MirrorDownloadRate {
                download_rate_err,
                mirror,
                debname,
                client_ip,
            }) => {
                write!(
                    f,
                    "Timeout occurred for mirror {} downloading file {} for client {} after a download rate of {} for the last {} seconds",
                    mirror,
                    debname,
                    client_ip.to_canonical(),
                    HumanFmt::Rate(
                        download_rate_err.download_size as u64,
                        Duration::from_secs(download_rate_err.timeframe.get() as u64)
                    ),
                    download_rate_err.timeframe,
                )
            }
            Self::Memfd(e) => e.fmt(f),
            Self::ContentTooLarge(orig, datalen) => {
                write!(
                    f,
                    "Received data of {datalen} bytes larger than announced {orig}"
                )
            }
        }
    }
}

impl std::error::Error for ProxyCacheError {}

impl From<std::io::Error> for ProxyCacheError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<hyper::Error> for ProxyCacheError {
    fn from(value: hyper::Error) -> Self {
        Self::Hyper(value)
    }
}

impl From<hyper_util::client::legacy::Error> for ProxyCacheError {
    fn from(value: hyper_util::client::legacy::Error) -> Self {
        Self::HyperUtil(value)
    }
}

impl From<sqlx::Error> for ProxyCacheError {
    fn from(value: sqlx::Error) -> Self {
        Self::Sqlx(value)
    }
}

impl From<std::time::SystemTimeError> for ProxyCacheError {
    fn from(value: std::time::SystemTimeError) -> Self {
        Self::SystemTime(value)
    }
}

impl From<std::string::FromUtf8Error> for ProxyCacheError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::Utf8(value)
    }
}

impl From<ChannelBodyError> for ProxyCacheError {
    fn from(value: ChannelBodyError) -> Self {
        match value {
            ChannelBodyError::MirrorDownloadRate(mdr) => Self::MirrorDownloadRate(mdr),
        }
    }
}
