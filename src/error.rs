use std::{net::IpAddr, num::NonZero, time::Duration};

use crate::ContentLength;
use crate::{ChannelBodyError, deb_mirror::Mirror, humanfmt::HumanFmt};

#[derive(Clone, Debug)]
pub(crate) struct ClientDownloadRate {
    pub(crate) total_size: usize,
    pub(crate) timeout_secs: NonZero<usize>,
    pub(crate) client_ip: IpAddr,
}

#[derive(Clone, Debug)]
pub(crate) struct MirrorDownloadRate {
    pub(crate) total_size: usize,
    pub(crate) timeout_secs: NonZero<usize>,
    pub(crate) mirror: Mirror,
    pub(crate) debname: String,
    pub(crate) client_ip: IpAddr,
}

#[derive(Debug)]
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
    #[expect(dead_code)]
    Generic(String),
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
                total_size: total,
                timeout_secs,
                client_ip,
            }) => {
                write!(
                    f,
                    "Timeout occurred for client {} after a download rate of {} for the last {} seconds",
                    client_ip.to_canonical(),
                    HumanFmt::Rate(
                        *total as u64,
                        Duration::from_secs((*timeout_secs).get() as u64)
                    ),
                    timeout_secs,
                )
            }
            Self::MirrorDownloadRate(MirrorDownloadRate {
                total_size,
                timeout_secs,
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
                        *total_size as u64,
                        Duration::from_secs((*timeout_secs).get() as u64)
                    ),
                    timeout_secs,
                )
            }
            Self::Memfd(e) => e.fmt(f),
            Self::ContentTooLarge(orig, datalen) => {
                write!(
                    f,
                    "Received data of {} too larger than announced {:?}",
                    HumanFmt::Size(*datalen),
                    orig,
                )
            }
            Self::Generic(msg) => msg.fmt(f),
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
            ChannelBodyError::ClientDownloadRate(cdr) => Self::ClientDownloadRate(cdr),
            ChannelBodyError::MirrorDownloadRate(mdr) => Self::MirrorDownloadRate(mdr),
        }
    }
}
