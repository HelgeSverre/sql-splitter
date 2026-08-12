//! Exact PostgreSQL binary codecs for canonical migration values.

use thiserror::Error;

use super::model::DbValue;

const POSTGRES_EPOCH_UNIX_DAYS: i64 = 10_957;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SECOND;
const NANOS_PER_MICRO: i128 = 1_000;
const NUMERIC_POSITIVE: u16 = 0x0000;
const NUMERIC_NEGATIVE: u16 = 0x4000;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum PostgresCodecError {
    #[error("invalid PostgreSQL binary {kind} value")]
    InvalidBinary { kind: &'static str },
    #[error("canonical {kind} value cannot be encoded for PostgreSQL")]
    InvalidCanonical { kind: &'static str },
    #[error("PostgreSQL {kind} value is non-finite")]
    NonFinite { kind: &'static str },
}

pub(crate) fn decode_numeric(
    bytes: &[u8],
    expected_precision: u32,
    expected_scale: i32,
) -> Result<DbValue, PostgresCodecError> {
    if bytes.len() < 8
        || !bytes.len().is_multiple_of(2)
        || !(1..=65).contains(&expected_precision)
        || !(0..=30).contains(&expected_scale)
        || u32::try_from(expected_scale).is_ok_and(|scale| scale > expected_precision)
    {
        return Err(invalid_binary("numeric"));
    }
    let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]);
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    let dscale = u16::from_be_bytes([bytes[6], bytes[7]]);
    if ndigits < 0
        || usize::try_from(ndigits)
            .ok()
            .and_then(|count| count.checked_mul(2))
            .and_then(|length| length.checked_add(8))
            != Some(bytes.len())
        || i32::from(dscale) != expected_scale
    {
        return Err(invalid_binary("numeric"));
    }
    if !matches!(sign, NUMERIC_POSITIVE | NUMERIC_NEGATIVE) {
        return Err(PostgresCodecError::NonFinite { kind: "numeric" });
    }
    let digits = bytes[8..]
        .chunks_exact(2)
        .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>();
    if digits.iter().any(|digit| *digit > 9_999) {
        return Err(invalid_binary("numeric"));
    }

    let fractional_groups = (expected_scale + 3) / 4;
    let lowest_retained_position = -fractional_groups;
    if digits.iter().enumerate().any(|(index, digit)| {
        let position = i32::from(weight) - i32::try_from(index).unwrap_or(i32::MAX);
        position < lowest_retained_position && *digit != 0
    }) {
        return Err(invalid_binary("numeric"));
    }

    let digit_at = |position: i32| {
        let index = i32::from(weight) - position;
        usize::try_from(index)
            .ok()
            .and_then(|index| digits.get(index))
            .copied()
            .unwrap_or(0)
    };
    let mut decimal_digits = String::new();
    if weight >= 0 {
        for position in (0..=i32::from(weight)).rev() {
            decimal_digits.push_str(&format!("{:04}", digit_at(position)));
        }
    } else {
        decimal_digits.push('0');
    }
    for position in (lowest_retained_position..=-1).rev() {
        decimal_digits.push_str(&format!("{:04}", digit_at(position)));
    }
    let scale = usize::try_from(expected_scale).map_err(|_| invalid_binary("numeric"))?;
    if !scale.is_multiple_of(4) {
        let remove = 4 - scale % 4;
        let discarded_divisor =
            10_u16.pow(u32::try_from(remove).map_err(|_| invalid_binary("numeric"))?);
        if digit_at(lowest_retained_position) % discarded_divisor != 0 {
            return Err(invalid_binary("numeric"));
        }
        decimal_digits.truncate(
            decimal_digits
                .len()
                .checked_sub(remove)
                .ok_or_else(|| invalid_binary("numeric"))?,
        );
    }
    let coefficient = decimal_digits.trim_start_matches('0');
    let mut coefficient = if coefficient.is_empty() {
        vec![b'0']
    } else {
        coefficient.as_bytes().to_vec()
    };
    if sign == NUMERIC_NEGATIVE && coefficient != b"0" {
        coefficient.insert(0, b'-');
    }
    let coefficient_digits = coefficient.strip_prefix(b"-").unwrap_or(&coefficient);
    if u32::try_from(coefficient_digits.len())
        .ok()
        .is_none_or(|digits| digits > expected_precision)
    {
        return Err(invalid_binary("numeric"));
    }
    Ok(DbValue::Decimal {
        coefficient,
        scale: expected_scale,
    })
}

pub(crate) fn encode_numeric(
    coefficient: &[u8],
    scale: i32,
) -> Result<Vec<u8>, PostgresCodecError> {
    if !(0..=30).contains(&scale) {
        return Err(invalid_canonical("numeric"));
    }
    let (negative, digits) = coefficient
        .strip_prefix(b"-")
        .map_or((false, coefficient), |digits| (true, digits));
    if digits.is_empty()
        || !digits.iter().all(u8::is_ascii_digit)
        || (digits.len() > 1 && digits.first() == Some(&b'0'))
        || (negative && digits == b"0")
        || digits.len() > 65
    {
        return Err(invalid_canonical("numeric"));
    }
    let scale = usize::try_from(scale).map_err(|_| invalid_canonical("numeric"))?;
    let integer_len = digits.len().saturating_sub(scale);
    let mut integer = if integer_len == 0 {
        vec![b'0']
    } else {
        digits[..integer_len].to_vec()
    };
    let mut fraction = vec![b'0'; scale.saturating_sub(digits.len())];
    if scale != 0 {
        fraction.extend_from_slice(&digits[integer_len..]);
    }
    let integer_padding = (4 - integer.len() % 4) % 4;
    integer.splice(0..0, std::iter::repeat_n(b'0', integer_padding));
    let fractional_padding = (4 - fraction.len() % 4) % 4;
    fraction.extend(std::iter::repeat_n(b'0', fractional_padding));
    let integer_groups = integer.len() / 4;
    let mut groups = integer
        .chunks_exact(4)
        .chain(fraction.chunks_exact(4))
        .map(parse_decimal_group)
        .collect::<Result<Vec<_>, _>>()?;
    let leading_zero_groups = groups.iter().take_while(|digit| **digit == 0).count();
    let trailing_zero_groups = groups.iter().rev().take_while(|digit| **digit == 0).count();
    let keep_end = groups.len().saturating_sub(trailing_zero_groups);
    groups = if leading_zero_groups >= keep_end {
        Vec::new()
    } else {
        groups[leading_zero_groups..keep_end].to_vec()
    };
    let weight = if groups.is_empty() {
        0
    } else {
        i32::try_from(integer_groups)
            .ok()
            .and_then(|groups| groups.checked_sub(1))
            .and_then(|weight| weight.checked_sub(i32::try_from(leading_zero_groups).ok()?))
            .and_then(|weight| i16::try_from(weight).ok())
            .ok_or_else(|| invalid_canonical("numeric"))?
    };
    let ndigits = i16::try_from(groups.len()).map_err(|_| invalid_canonical("numeric"))?;
    let dscale = u16::try_from(scale).map_err(|_| invalid_canonical("numeric"))?;
    let mut encoded = Vec::with_capacity(8 + groups.len() * 2);
    encoded.extend_from_slice(&ndigits.to_be_bytes());
    encoded.extend_from_slice(&weight.to_be_bytes());
    encoded.extend_from_slice(
        &(if negative && !groups.is_empty() {
            NUMERIC_NEGATIVE
        } else {
            NUMERIC_POSITIVE
        })
        .to_be_bytes(),
    );
    encoded.extend_from_slice(&dscale.to_be_bytes());
    for group in groups {
        encoded.extend_from_slice(&group.to_be_bytes());
    }
    Ok(encoded)
}

pub(crate) fn decode_date(bytes: &[u8]) -> Result<DbValue, PostgresCodecError> {
    let days = decode_i32(bytes, "date")?;
    if matches!(days, i32::MIN | i32::MAX) {
        return Err(PostgresCodecError::NonFinite { kind: "date" });
    }
    let (year, month, day) = civil_from_days(i64::from(days) + POSTGRES_EPOCH_UNIX_DAYS)
        .ok_or_else(|| invalid_binary("date"))?;
    Ok(DbValue::Date { year, month, day })
}

pub(crate) fn encode_date(year: i32, month: u8, day: u8) -> Result<Vec<u8>, PostgresCodecError> {
    let days = days_from_civil(year, month, day)
        .and_then(|days| days.checked_sub(POSTGRES_EPOCH_UNIX_DAYS))
        .and_then(|days| i32::try_from(days).ok())
        .ok_or_else(|| invalid_canonical("date"))?;
    Ok(days.to_be_bytes().to_vec())
}

pub(crate) fn decode_time(bytes: &[u8]) -> Result<DbValue, PostgresCodecError> {
    let micros = decode_i64(bytes, "time")?;
    if !(0..=MICROS_PER_DAY).contains(&micros) {
        return Err(invalid_binary("time"));
    }
    Ok(DbValue::Time {
        nanos: i128::from(micros) * NANOS_PER_MICRO,
    })
}

pub(crate) fn encode_time(nanos: i128) -> Result<Vec<u8>, PostgresCodecError> {
    if nanos.rem_euclid(NANOS_PER_MICRO) != 0 {
        return Err(invalid_canonical("time"));
    }
    let micros = i64::try_from(nanos / NANOS_PER_MICRO)
        .ok()
        .filter(|value| (0..=MICROS_PER_DAY).contains(value))
        .ok_or_else(|| invalid_canonical("time"))?;
    Ok(micros.to_be_bytes().to_vec())
}

pub(crate) fn decode_timestamp(
    bytes: &[u8],
    precision: u8,
    offset_aware: bool,
) -> Result<DbValue, PostgresCodecError> {
    if precision > 6 {
        return Err(invalid_binary("timestamp"));
    }
    let micros = decode_i64(bytes, "timestamp")?;
    if matches!(micros, i64::MIN | i64::MAX) {
        return Err(PostgresCodecError::NonFinite { kind: "timestamp" });
    }
    let days = micros.div_euclid(MICROS_PER_DAY);
    let day_micros = micros.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = civil_from_days(days + POSTGRES_EPOCH_UNIX_DAYS)
        .ok_or_else(|| invalid_binary("timestamp"))?;
    let hour = day_micros / (3_600 * MICROS_PER_SECOND);
    let minute = day_micros / (60 * MICROS_PER_SECOND) % 60;
    let second = day_micros / MICROS_PER_SECOND % 60;
    let fraction = day_micros % MICROS_PER_SECOND;
    let quantum = 10_i64.pow(u32::from(6 - precision));
    if fraction.rem_euclid(quantum) != 0 {
        return Err(invalid_binary("timestamp"));
    }
    Ok(DbValue::Timestamp {
        local: format_timestamp(year, month, day, hour, minute, second, fraction),
        offset_minutes: offset_aware.then_some(0),
        precision,
    })
}

pub(crate) fn encode_timestamp(
    local: &str,
    offset_minutes: Option<i16>,
    precision: u8,
    offset_aware: bool,
) -> Result<Vec<u8>, PostgresCodecError> {
    if precision > 6
        || offset_aware != offset_minutes.is_some()
        || offset_minutes.is_some_and(|offset| !(-14 * 60..=14 * 60).contains(&offset))
    {
        return Err(invalid_canonical("timestamp"));
    }
    let timestamp = parse_timestamp(local)?;
    let quantum = 10_u32.pow(u32::from(9 - precision));
    if !timestamp.nanos.is_multiple_of(quantum) || !timestamp.nanos.is_multiple_of(1_000) {
        return Err(invalid_canonical("timestamp"));
    }
    let days = days_from_civil(timestamp.year, timestamp.month, timestamp.day)
        .and_then(|days| days.checked_sub(POSTGRES_EPOCH_UNIX_DAYS))
        .ok_or_else(|| invalid_canonical("timestamp"))?;
    let day_micros = i64::from(timestamp.hour) * 3_600 * MICROS_PER_SECOND
        + i64::from(timestamp.minute) * 60 * MICROS_PER_SECOND
        + i64::from(timestamp.second) * MICROS_PER_SECOND
        + i64::from(timestamp.nanos / 1_000);
    let offset_micros = i64::from(offset_minutes.unwrap_or(0)) * 60 * MICROS_PER_SECOND;
    let micros = days
        .checked_mul(MICROS_PER_DAY)
        .and_then(|value| value.checked_add(day_micros))
        .and_then(|value| value.checked_sub(offset_micros))
        .ok_or_else(|| invalid_canonical("timestamp"))?;
    Ok(micros.to_be_bytes().to_vec())
}

fn parse_decimal_group(group: &[u8]) -> Result<u16, PostgresCodecError> {
    let text = std::str::from_utf8(group).map_err(|_| invalid_canonical("numeric"))?;
    text.parse().map_err(|_| invalid_canonical("numeric"))
}

fn decode_i32(bytes: &[u8], kind: &'static str) -> Result<i32, PostgresCodecError> {
    bytes
        .try_into()
        .map(i32::from_be_bytes)
        .map_err(|_| invalid_binary(kind))
}

fn decode_i64(bytes: &[u8], kind: &'static str) -> Result<i64, PostgresCodecError> {
    bytes
        .try_into()
        .map(i64::from_be_bytes)
        .map_err(|_| invalid_binary(kind))
}

#[derive(Debug, Clone, Copy)]
struct TimestampParts {
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanos: u32,
}

fn parse_timestamp(local: &str) -> Result<TimestampParts, PostgresCodecError> {
    let (date, time) = local
        .split_once(' ')
        .ok_or_else(|| invalid_canonical("timestamp"))?;
    let (year, month, day) = parse_date(date)?;
    let mut parts = time.split(':');
    let hour = parse_u8(parts.next())?;
    let minute = parse_u8(parts.next())?;
    let second_fraction = parts.next().ok_or_else(|| invalid_canonical("timestamp"))?;
    if parts.next().is_some() {
        return Err(invalid_canonical("timestamp"));
    }
    let (second, nanos) = if let Some((second, fraction)) = second_fraction.split_once('.') {
        if fraction.is_empty()
            || fraction.len() > 9
            || !fraction.as_bytes().iter().all(u8::is_ascii_digit)
        {
            return Err(invalid_canonical("timestamp"));
        }
        let mut nanos = fraction
            .parse::<u32>()
            .map_err(|_| invalid_canonical("timestamp"))?;
        for _ in fraction.len()..9 {
            nanos = nanos
                .checked_mul(10)
                .ok_or_else(|| invalid_canonical("timestamp"))?;
        }
        (
            second
                .parse::<u8>()
                .map_err(|_| invalid_canonical("timestamp"))?,
            nanos,
        )
    } else {
        (
            second_fraction
                .parse()
                .map_err(|_| invalid_canonical("timestamp"))?,
            0,
        )
    };
    if hour > 23 || minute > 59 || second > 59 {
        return Err(invalid_canonical("timestamp"));
    }
    Ok(TimestampParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanos,
    })
}

fn parse_date(date: &str) -> Result<(i32, u8, u8), PostgresCodecError> {
    let split = date.rfind('-').ok_or_else(|| invalid_canonical("date"))?;
    let (year_month, day) = date.split_at(split);
    let day = day[1..]
        .parse::<u8>()
        .map_err(|_| invalid_canonical("date"))?;
    let split = year_month
        .rfind('-')
        .ok_or_else(|| invalid_canonical("date"))?;
    let (year, month) = year_month.split_at(split);
    let year = year.parse::<i32>().map_err(|_| invalid_canonical("date"))?;
    if year == 0 && year_month.starts_with('-') {
        return Err(invalid_canonical("date"));
    }
    let month = month[1..]
        .parse::<u8>()
        .map_err(|_| invalid_canonical("date"))?;
    valid_date(year, month, day)
        .then_some((year, month, day))
        .ok_or_else(|| invalid_canonical("date"))
}

fn parse_u8(value: Option<&str>) -> Result<u8, PostgresCodecError> {
    value
        .ok_or_else(|| invalid_canonical("timestamp"))?
        .parse()
        .map_err(|_| invalid_canonical("timestamp"))
}

fn format_timestamp(
    year: i32,
    month: u8,
    day: u8,
    hour: i64,
    minute: i64,
    second: i64,
    micros: i64,
) -> String {
    let year = if year < 0 {
        format!("-{:04}", i64::from(year).unsigned_abs())
    } else {
        format!("{year:04}")
    };
    format!("{year}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}")
}

fn days_from_civil(year: i32, month: u8, day: u8) -> Option<i64> {
    if !valid_date(year, month, day) {
        return None;
    }
    let mut year = i64::from(year);
    let month = i64::from(month);
    let day = i64::from(day);
    year -= i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146_097 + day_of_era - 719_468)
}

fn civil_from_days(days: i64) -> Option<(i32, u8, u8)> {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    Some((
        i32::try_from(year).ok()?,
        u8::try_from(month).ok()?,
        u8::try_from(day).ok()?,
    ))
}

fn valid_date(year: i32, month: u8, day: u8) -> bool {
    let leap = year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0);
    let maximum = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    day != 0 && day <= maximum
}

fn invalid_binary(kind: &'static str) -> PostgresCodecError {
    PostgresCodecError::InvalidBinary { kind }
}

fn invalid_canonical(kind: &'static str) -> PostgresCodecError {
    PostgresCodecError::InvalidCanonical { kind }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_binary_round_trips_exact_scale_and_sign() {
        for value in [
            DbValue::Decimal {
                coefficient: b"0".to_vec(),
                scale: 0,
            },
            DbValue::Decimal {
                coefficient: b"12345".to_vec(),
                scale: 2,
            },
            DbValue::Decimal {
                coefficient: b"12".to_vec(),
                scale: 4,
            },
            DbValue::Decimal {
                coefficient: b"-90000000000000000001".to_vec(),
                scale: 0,
            },
            DbValue::Decimal {
                coefficient: b"100000000".to_vec(),
                scale: 8,
            },
        ] {
            let DbValue::Decimal { coefficient, scale } = &value else {
                unreachable!()
            };
            let encoded = encode_numeric(coefficient, *scale).unwrap();
            let coefficient_digits =
                u32::try_from(coefficient.strip_prefix(b"-").unwrap_or(coefficient).len()).unwrap();
            let precision = coefficient_digits.max(u32::try_from(*scale).unwrap());
            assert_eq!(decode_numeric(&encoded, precision, *scale).unwrap(), value);
        }
    }

    #[test]
    fn numeric_binary_matches_postgresql_base_10000_wire_vectors() {
        let positive = [0, 2, 0, 0, 0, 0, 0, 2, 0, 123, 0x11, 0x94];
        assert_eq!(
            decode_numeric(&positive, 5, 2).unwrap(),
            DbValue::Decimal {
                coefficient: b"12345".to_vec(),
                scale: 2,
            }
        );
        assert_eq!(encode_numeric(b"12345", 2).unwrap(), positive);

        let negative_fraction = [0, 1, 0xff, 0xff, 0x40, 0, 0, 4, 0, 12];
        assert_eq!(
            decode_numeric(&negative_fraction, 4, 4).unwrap(),
            DbValue::Decimal {
                coefficient: b"-12".to_vec(),
                scale: 4,
            }
        );
        assert_eq!(encode_numeric(b"-12", 4).unwrap(), negative_fraction);
        assert_eq!(encode_numeric(b"0", 2).unwrap(), [0, 0, 0, 0, 0, 0, 0, 2]);
    }

    #[test]
    fn numeric_rejects_nonfinite_and_noncanonical_values() {
        let nan = [0, 0, 0, 0, 0xc0, 0, 0, 0];
        assert!(matches!(
            decode_numeric(&nan, 1, 0),
            Err(PostgresCodecError::NonFinite { .. })
        ));
        assert!(encode_numeric(b"-0", 0).is_err());
        assert!(encode_numeric(b"01", 0).is_err());
        assert!(encode_numeric(b"1", 31).is_err());
        let nonzero_subscale_digits = [0, 2, 0, 0, 0, 0, 0, 2, 0, 123, 0x11, 0xf7];
        assert!(matches!(
            decode_numeric(&nonzero_subscale_digits, 5, 2),
            Err(PostgresCodecError::InvalidBinary { .. })
        ));
    }

    #[test]
    fn date_binary_round_trips_epoch_leap_bc_and_postgres_edges() {
        assert_eq!(encode_date(2_000, 1, 1).unwrap(), [0, 0, 0, 0]);
        assert_eq!(
            encode_date(1_970, 1, 1).unwrap(),
            (-10_957_i32).to_be_bytes()
        );
        for value in [
            (-4_712, 1, 1),
            (0, 2, 29),
            (1_970, 1, 1),
            (2_000, 1, 1),
            (2_000, 2, 29),
            (294_276, 12, 31),
        ] {
            let encoded = encode_date(value.0, value.1, value.2).unwrap();
            assert_eq!(
                decode_date(&encoded).unwrap(),
                DbValue::Date {
                    year: value.0,
                    month: value.1,
                    day: value.2
                }
            );
        }
    }

    #[test]
    fn time_binary_round_trips_boundaries_and_requires_microseconds() {
        assert_eq!(encode_time(1_000).unwrap(), 1_i64.to_be_bytes());
        for nanos in [0, 1_000, 86_399_999_999_000, 86_400_000_000_000] {
            assert_eq!(
                decode_time(&encode_time(nanos).unwrap()).unwrap(),
                DbValue::Time { nanos }
            );
        }
        assert!(encode_time(1).is_err());
        assert!(encode_time(-1_000).is_err());
    }

    #[test]
    fn timestamp_binary_round_trips_wide_years_and_offsets() {
        assert_eq!(
            encode_timestamp("2000-01-01 00:00:00.000000", None, 6, false).unwrap(),
            0_i64.to_be_bytes()
        );
        assert_eq!(
            encode_timestamp("1970-01-01 00:00:00.000000", None, 6, false).unwrap(),
            (-946_684_800_000_000_i64).to_be_bytes()
        );
        for value in [
            ("-4712-01-01 00:00:00.000000", None, 6, false),
            ("1970-01-01 00:00:00.000001", None, 6, false),
            ("2000-02-29 23:59:59.123000", None, 3, false),
            ("2038-01-19 03:14:07.999999", Some(0), 6, true),
            ("294276-12-31 23:59:59.999999", None, 6, false),
        ] {
            let encoded = encode_timestamp(value.0, value.1, value.2, value.3).unwrap();
            assert_eq!(
                decode_timestamp(&encoded, value.2, value.3).unwrap(),
                DbValue::Timestamp {
                    local: value.0.into(),
                    offset_minutes: value.1,
                    precision: value.2,
                }
            );
        }
    }

    #[test]
    fn timestamp_offset_is_normalized_to_utc_on_decode() {
        let encoded = encode_timestamp("2000-01-01 01:00:00.000000", Some(60), 6, true).unwrap();
        assert_eq!(
            decode_timestamp(&encoded, 6, true).unwrap(),
            DbValue::Timestamp {
                local: "2000-01-01 00:00:00.000000".into(),
                offset_minutes: Some(0),
                precision: 6,
            }
        );
    }
}
