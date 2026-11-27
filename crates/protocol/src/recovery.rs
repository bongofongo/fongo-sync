use sha2::{Digest, Sha256};
use thiserror::Error;
use zeroize::Zeroize;

use crate::keys::{LibraryId, LibraryKeys};

const VERSION: u8 = 1;
const LABEL: &[u8] = b"fongo-sync/v1/recovery\0";
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const PAYLOAD: usize = 1 + 16 + 32;
const CHECKSUM: usize = 4;
const BYTES: usize = PAYLOAD + CHECKSUM;
const SYMBOLS: usize = (BYTES * 8).div_ceil(5);
const GROUP: usize = 5;

/// Why a recovery code was refused.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RecoveryError {
    #[error("{0:?} is not a recovery code character")]
    InvalidCharacter(char),
    #[error("a recovery code has {SYMBOLS} characters, not {0}")]
    Length(usize),
    #[error("the code has a mistake in it")]
    Checksum,
    #[error("recovery code version {0} is not supported")]
    UnsupportedVersion(u8),
}

impl LibraryKeys {
    /// The library id and secret as 85 Crockford base32 characters in hyphenated groups of five.
    /// Whoever holds it holds the library.
    pub fn recovery_code(&self) -> String {
        let mut bytes = [0u8; BYTES];
        bytes[0] = VERSION;
        bytes[1..17].copy_from_slice(&self.id.0);
        bytes[17..PAYLOAD].copy_from_slice(self.secret());
        let sum = checksum(&bytes[..PAYLOAD]);
        bytes[PAYLOAD..].copy_from_slice(&sum);
        let mut code = String::with_capacity(SYMBOLS + SYMBOLS / GROUP);
        for i in 0..SYMBOLS {
            if i > 0 && i % GROUP == 0 {
                code.push('-');
            }
            code.push(ALPHABET[usize::from(symbol_at(&bytes, i))] as char);
        }
        bytes.zeroize();
        code
    }

    /// Reads a recovery code, ignoring case, whitespace and hyphens, and reading O as 0 and I or L
    /// as 1.
    pub fn from_recovery_code(code: &str) -> Result<LibraryKeys, RecoveryError> {
        let mut bytes = [0u8; BYTES];
        let result = read_code(code, &mut bytes);
        let keys = result.map(|()| {
            let mut id = [0u8; 16];
            let mut secret = [0u8; 32];
            id.copy_from_slice(&bytes[1..17]);
            secret.copy_from_slice(&bytes[17..PAYLOAD]);
            LibraryKeys::new(LibraryId(id), secret)
        });
        bytes.zeroize();
        keys
    }
}

fn read_code(code: &str, bytes: &mut [u8; BYTES]) -> Result<(), RecoveryError> {
    let mut n = 0usize;
    let mut padding = 0u8;
    for c in code.chars() {
        if c == '-' || c.is_whitespace() {
            continue;
        }
        let value = value_of(c).ok_or(RecoveryError::InvalidCharacter(c))?;
        if n < SYMBOLS {
            padding = put_symbol(bytes, n, value);
        }
        n += 1;
    }
    if n != SYMBOLS {
        return Err(RecoveryError::Length(n));
    }
    if padding != 0 || bytes[PAYLOAD..] != checksum(&bytes[..PAYLOAD]) {
        return Err(RecoveryError::Checksum);
    }
    if bytes[0] != VERSION {
        return Err(RecoveryError::UnsupportedVersion(bytes[0]));
    }
    Ok(())
}

fn value_of(c: char) -> Option<u8> {
    let c = match c.to_ascii_uppercase() {
        'O' => '0',
        'I' | 'L' => '1',
        c => c,
    };
    ALPHABET
        .iter()
        .position(|&a| char::from(a) == c)
        .map(|i| i as u8)
}

fn checksum(payload: &[u8]) -> [u8; CHECKSUM] {
    let digest = Sha256::new()
        .chain_update(LABEL)
        .chain_update(payload)
        .finalize();
    let mut sum = [0u8; CHECKSUM];
    sum.copy_from_slice(&digest[..CHECKSUM]);
    sum
}

fn bit(bytes: &[u8], i: usize) -> u8 {
    bytes.get(i / 8).map_or(0, |b| (b >> (7 - i % 8)) & 1)
}

fn symbol_at(bytes: &[u8], n: usize) -> u8 {
    (0..5).fold(0, |acc, k| (acc << 1) | bit(bytes, n * 5 + k))
}

/// Writes symbol `n`'s bits into `bytes`, returning the bits that fell past the end.
fn put_symbol(bytes: &mut [u8], n: usize, value: u8) -> u8 {
    let mut overflow = 0;
    for k in 0..5 {
        let b = (value >> (4 - k)) & 1;
        let i = n * 5 + k;
        match bytes.get_mut(i / 8) {
            Some(byte) => *byte |= b << (7 - i % 8),
            None => overflow |= b,
        }
    }
    overflow
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn arb_keys() -> impl Strategy<Value = LibraryKeys> {
        (any::<[u8; 16]>(), any::<[u8; 32]>())
            .prop_map(|(id, s)| LibraryKeys::new(LibraryId(id), s))
    }

    fn same(a: &LibraryKeys, b: &LibraryKeys) -> bool {
        a.id == b.id && a.secret() == b.secret()
    }

    proptest! {
        #![proptest_config(crate::proptest_config())]

        #[test]
        fn any_code_round_trips(keys in arb_keys()) {
            let code = keys.recovery_code();
            prop_assert_eq!(code.chars().filter(|&c| c != '-').count(), SYMBOLS);
            prop_assert!(same(&keys, &LibraryKeys::from_recovery_code(&code).unwrap()));
        }

        #[test]
        fn a_sloppy_transcription_still_reads(keys in arb_keys()) {
            let sloppy: String = keys
                .recovery_code()
                .chars()
                .map(|c| match c {
                    '0' => 'o',
                    '1' => 'l',
                    '-' => ' ',
                    c => c.to_ascii_lowercase(),
                })
                .collect();
            prop_assert!(same(&keys, &LibraryKeys::from_recovery_code(&sloppy).unwrap()));
        }

        #[test]
        fn a_single_wrong_character_is_detected(
            keys in arb_keys(),
            at in any::<prop::sample::Index>(),
            shift in 1u8..32,
        ) {
            let mut symbols: Vec<u8> = keys.recovery_code().bytes().filter(|&c| c != b'-').collect();
            let i = at.index(symbols.len());
            let old = value_of(char::from(symbols[i])).unwrap();
            symbols[i] = ALPHABET[usize::from((old + shift) % 32)];
            let code = String::from_utf8(symbols).unwrap();
            prop_assert_eq!(LibraryKeys::from_recovery_code(&code).err(), Some(RecoveryError::Checksum));
        }

        #[test]
        fn decoding_arbitrary_text_never_panics(text in ".{0,120}") {
            let _ = LibraryKeys::from_recovery_code(&text);
        }
    }

    #[test]
    fn wrong_lengths_and_characters_are_named() {
        let code = LibraryKeys::new(LibraryId([1; 16]), [2; 32]).recovery_code();
        assert_eq!(
            LibraryKeys::from_recovery_code(&code[1..]).err(),
            Some(RecoveryError::Length(SYMBOLS - 1))
        );
        assert_eq!(
            LibraryKeys::from_recovery_code(&format!("{code}0")).err(),
            Some(RecoveryError::Length(SYMBOLS + 1))
        );
        assert_eq!(
            LibraryKeys::from_recovery_code(&format!("U{}", &code[1..])).err(),
            Some(RecoveryError::InvalidCharacter('U'))
        );
        assert_eq!(SYMBOLS, 85);
    }
}
