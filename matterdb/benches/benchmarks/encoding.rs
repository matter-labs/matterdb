use std::{borrow::Cow, fmt, hint::black_box};

use criterion::{Bencher, Criterion};
use matterdb::BinaryValue;
use rand::{RngCore, SeedableRng, rngs::StdRng};

const CHUNK_SIZE: usize = 64;
const SEED: [u8; 32] = [100; 32];

#[derive(Debug, Clone, Copy, PartialEq)]
struct SimpleData {
    id: u16,
    class: i16,
    value: i32,
}

impl BinaryValue for SimpleData {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = vec![0; 8];
        buffer[0..2].copy_from_slice(&self.id.to_le_bytes());
        buffer[2..4].copy_from_slice(&self.class.to_le_bytes());
        buffer[4..8].copy_from_slice(&self.value.to_le_bytes());
        buffer
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> anyhow::Result<Self> {
        let bytes = bytes.as_ref();
        let id = u16::from_le_bytes(bytes[0..2].try_into().unwrap());
        let class = i16::from_le_bytes(bytes[2..4].try_into().unwrap());
        let value = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
        Ok(Self { id, class, value })
    }
}

fn gen_bytes_data() -> Vec<u8> {
    let mut rng: StdRng = SeedableRng::from_seed(SEED);
    let mut v = vec![0; CHUNK_SIZE];
    rng.fill_bytes(&mut v);
    v
}

fn check_binary_value<T>(data: T) -> T
where
    T: BinaryValue + fmt::Debug + PartialEq,
{
    let bytes = data.to_bytes();
    assert_eq!(T::from_bytes(bytes.into()).unwrap(), data);
    data
}

fn gen_sample_data() -> SimpleData {
    check_binary_value(SimpleData {
        id: 1,
        class: -5,
        value: 2127,
    })
}

fn bench_binary_value<F, V>(c: &mut Criterion, name: &str, f: F)
where
    F: Fn() -> V + 'static + Clone + Copy,
    V: BinaryValue + PartialEq + fmt::Debug,
{
    // Checks that binary value is correct.
    let val = f();
    let bytes = val.to_bytes();
    let val2 = V::from_bytes(bytes.into()).unwrap();
    assert_eq!(val, val2);
    // Runs benchmarks.
    c.bench_function(
        &format!("encoding/{name}/to_bytes"),
        move |b: &mut Bencher<'_>| {
            b.iter_with_setup(f, |data| black_box(data.to_bytes()));
        },
    );
    c.bench_function(
        &format!("encoding/{name}/into_bytes"),
        move |b: &mut Bencher<'_>| {
            b.iter_with_setup(f, |data| black_box(data.into_bytes()));
        },
    );
    c.bench_function(
        &format!("encoding/{name}/from_bytes"),
        move |b: &mut Bencher<'_>| {
            b.iter_with_setup(
                || {
                    let val = f();
                    val.to_bytes().into()
                },
                |bytes| black_box(V::from_bytes(bytes).unwrap()),
            );
        },
    );
}

pub(crate) fn bench_encoding(c: &mut Criterion) {
    bench_binary_value(c, "bytes", gen_bytes_data);
    bench_binary_value(c, "simple", gen_sample_data);
}
