#[must_use]
pub(crate) enum HumanFmt {
    Size(u64),
    Rate(u64, std::time::Duration),
    Time(std::time::Duration),
}

impl std::fmt::Display for HumanFmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[inline]
        #[must_use]
        const fn precision(size: f64) -> usize {
            if size > 100.0 {
                0
            } else if size > 10.0 {
                1
            } else {
                2
            }
        }

        #[expect(clippy::cast_precision_loss)]
        match *self {
            Self::Size(bytes) => {
                if bytes < 1000 {
                    return f.write_fmt(format_args!("{bytes}B"));
                }
                let size = bytes as f64 / 1000.0;
                if size < 1000.0 {
                    return f.write_fmt(format_args!("{size:.0$}kB", precision(size)));
                }
                let size = size / 1000.0;
                if size < 1000.0 {
                    return f.write_fmt(format_args!("{size:.0$}MB", precision(size)));
                }
                let size = size / 1000.0;
                if size < 1000.0 {
                    return f.write_fmt(format_args!("{size:.0$}GB", precision(size)));
                }
                let size = size / 1000.0;
                f.write_fmt(format_args!("{size:.0$}TB", precision(size)))
            }
            Self::Rate(bytes, time) => {
                let time = time.as_secs_f64();
                if time == 0.0 {
                    return f.write_fmt(format_args!("???B/s"));
                }
                let rate = bytes as f64 / time;
                if rate < 1000.0 {
                    return f.write_fmt(format_args!("{rate}B/s"));
                }
                let rate = rate / 1000.0;
                if rate < 1000.0 {
                    return f.write_fmt(format_args!("{rate:.0$}kB/s", precision(rate)));
                }
                let rate = rate / 1000.0;
                if rate < 1000.0 {
                    return f.write_fmt(format_args!("{rate:.0$}MB/s", precision(rate)));
                }
                let rate = rate / 1000.0;
                if rate < 1000.0 {
                    return f.write_fmt(format_args!("{rate:.0$}GB/s", precision(rate)));
                }
                let rate = rate / 1000.0;
                f.write_fmt(format_args!("{rate:.0$}TB/s", precision(rate)))
            }
            Self::Time(time) => {
                let time = time.as_nanos();
                if time < 1000 {
                    return f.write_fmt(format_args!("{time}ns"));
                }
                let time = time as f64 / 1000.0;
                if time < 1000.0 {
                    return f.write_fmt(format_args!("{time:.0$}us", precision(time)));
                }
                let time = time / 1000.0;
                if time < 1000.0 {
                    return f.write_fmt(format_args!("{time:.0$}ms", precision(time)));
                }
                let time = time / 1000.0;
                f.write_fmt(format_args!("{time:.0$}s", precision(time)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::humanfmt::HumanFmt;

    #[test]
    fn size_test() {
        assert_eq!(format!("{}", HumanFmt::Size(0)), "0B");
        assert_eq!(format!("{}", HumanFmt::Size(900)), "900B");
        assert_eq!(format!("{}", HumanFmt::Size(1024)), "1.02kB");
        assert_eq!(format!("{}", HumanFmt::Size(24756)), "24.8kB");
        assert_eq!(format!("{}", HumanFmt::Size(247_569_325_892)), "248GB");
        assert_eq!(format!("{}", HumanFmt::Size(u64::MAX)), "18446744TB");
    }

    #[test]
    fn rate_test() {
        assert_eq!(
            format!("{}", HumanFmt::Rate(0, Duration::from_nanos(0))),
            "???B/s"
        );
        assert_eq!(
            format!("{}", HumanFmt::Rate(1000, Duration::from_nanos(0))),
            "???B/s"
        );
        assert_eq!(
            format!("{}", HumanFmt::Rate(0, Duration::from_nanos(1000))),
            "0B/s"
        );
        assert_eq!(
            format!("{}", HumanFmt::Rate(1000, Duration::from_nanos(1000))),
            "1.00GB/s"
        );
        assert_eq!(
            format!(
                "{}",
                HumanFmt::Rate(u64::MAX, Duration::from_nanos(12_345_678_987_654_321))
            ),
            "1.49TB/s"
        );
    }

    #[test]
    fn time_test() {
        assert_eq!(
            format!("{}", HumanFmt::Time(Duration::from_nanos(0))),
            "0ns"
        );
        assert_eq!(
            format!("{}", HumanFmt::Time(Duration::from_nanos(900))),
            "900ns"
        );
        assert_eq!(
            format!("{}", HumanFmt::Time(Duration::from_nanos(1024))),
            "1.02us"
        );
        assert_eq!(
            format!("{}", HumanFmt::Time(Duration::from_nanos(24756))),
            "24.8us"
        );
        assert_eq!(
            format!("{}", HumanFmt::Time(Duration::from_nanos(247_569_325_892))),
            "248s"
        );
        assert_eq!(
            format!("{}", HumanFmt::Time(Duration::from_nanos(u64::MAX))),
            "18446744074s"
        );
    }
}
