use moonlink_rpc::{get_table_schema, scan_table_begin, scan_table_end};
use std::ffi::{c_char, CStr, CString};
use std::future::Future;
use std::result::Result as StdResult;
use std::sync::LazyLock;
use tokio::runtime::{Builder, Runtime};

#[repr(C)]
pub struct Data {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
}

#[repr(C)]
pub enum Result<T> {
    Ok(T),
    Err(*mut c_char),
}

pub type Stream = tokio::net::UnixStream;

#[repr(C)]
pub struct Void {
    _void: u8,
}

#[no_mangle]
pub extern "C" fn moonlink_connect(uri: *const c_char) -> Result<*mut Stream> {
    let uri = ptr_to_str(uri);
    block_on(Stream::connect(uri))
        .map(|stream| Box::into_raw(Box::new(stream)))
        .into()
}

#[no_mangle]
pub extern "C" fn moonlink_drop_cstring(cstring: *mut c_char) {
    unsafe { drop(CString::from_raw(cstring)) }
}

#[no_mangle]
pub extern "C" fn moonlink_drop_data(data: *mut Data) {
    let data = unsafe { Box::from_raw(data) };
    unsafe { Vec::from_raw_parts(data.ptr, data.len, data.capacity) };
}

#[no_mangle]
pub extern "C" fn moonlink_drop_stream(stream: *mut Stream) {
    unsafe { drop(Box::from_raw(stream)) }
}

#[no_mangle]
pub extern "C" fn moonlink_get_table_schema(
    stream: *mut Stream,
    database: *const c_char,
    schema: *const c_char,
    table: *const c_char,
) -> Result<*mut Data> {
    let stream = unsafe { &mut *stream };
    let database = ptr_to_str(database).to_owned();
    let table = format!("{}.{}", ptr_to_str(schema), ptr_to_str(table));
    block_on(get_table_schema(stream, database, table))
        .map(|schema| Box::into_raw(Box::new(schema.into())))
        .into()
}

#[no_mangle]
pub extern "C" fn moonlink_scan_table_begin(
    stream: *mut Stream,
    database: *const c_char,
    schema: *const c_char,
    table: *const c_char,
    lsn: u64,
) -> Result<*mut Data> {
    let stream = unsafe { &mut *stream };
    let database = ptr_to_str(database).to_owned();
    let table = format!("{}.{}", ptr_to_str(schema), ptr_to_str(table));
    block_on(scan_table_begin(stream, database, table, lsn))
        .map(|metadata| Box::into_raw(Box::new(metadata.into())))
        .into()
}

#[no_mangle]
pub extern "C" fn moonlink_scan_table_end(
    stream: *mut Stream,
    database: *const c_char,
    schema: *const c_char,
    table: *const c_char,
) -> Result<Void> {
    let stream = unsafe { &mut *stream };
    let database = ptr_to_str(database).to_owned();
    let table = format!("{}.{}", ptr_to_str(schema), ptr_to_str(table));
    block_on(scan_table_end(stream, database, table))
        .map(|unit| unit.into())
        .into()
}

fn ptr_to_str(ptr: *const c_char) -> &'static str {
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str().expect("DuckDB string should be valid UTF-8")
}

fn block_on<F: Future>(future: F) -> F::Output {
    static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Creating Tokio runtime should succeed")
    });
    RUNTIME.block_on(future)
}

impl From<Vec<u8>> for Data {
    fn from(v: Vec<u8>) -> Self {
        let mut v = std::mem::ManuallyDrop::new(v);
        Self {
            ptr: v.as_mut_ptr(),
            len: v.len(),
            capacity: v.capacity(),
        }
    }
}

impl<T, E: ToString> From<StdResult<T, E>> for Result<T> {
    fn from(res: StdResult<T, E>) -> Self {
        match res {
            Ok(t) => Self::Ok(t),
            Err(e) => {
                let cstring =
                    CString::new(e.to_string()).expect("Err should be convertible to CString");
                Self::Err(cstring.into_raw())
            }
        }
    }
}

impl From<()> for Void {
    fn from(_unit: ()) -> Self {
        Self { _void: 0 }
    }
}
