use std::ptr::null_mut;

use crate::native::{
    yacAllocHandle, yacFreeHandle, yacSetEnvAttr, EnYacCharsetCode_YAC_CHARSET_UTF8,
    EnYacEnvAttr_YAC_ATTR_CHARSET_CODE, EnYacHandleType_YAC_HANDLE_DBC,
    EnYacHandleType_YAC_HANDLE_ENV, EnYacHandleType_YAC_HANDLE_STMT, EnYacResult_YAC_ERROR,
    YacHandle,
};

use super::Error;

macro_rules! handle {
    ($name:ident => $handle_type:expr; $($input:ty)?) => {
        #[derive(Debug)]
        pub struct $name(pub YacHandle);

        unsafe impl Send for $name {}

        impl $name {
            pub fn new($(input: &$input)?) -> Result<Self, Error> {
                let get_input = ||{
                    $(
                        return (input as &$input).0;
                        #[allow(unreachable_code)]
                    )?
                    null_mut()
                };
                let handle: YacHandle = null_mut();
                let result = unsafe {
                    yacAllocHandle(
                        $handle_type,
                        get_input(),
                        &handle as *const _ as *mut _,
                    )
                };
                if result != EnYacResult_YAC_ERROR {
                    Ok(Self(handle))
                } else {
                    Err(Error::get_yas_diag(None).unwrap())
                }
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                unsafe {
                    yacFreeHandle($handle_type, self.0);
                }
            }
        }
    };
}

handle! {EnvHandle => EnYacHandleType_YAC_HANDLE_ENV;}
handle! {DbcHandle => EnYacHandleType_YAC_HANDLE_DBC; EnvHandle}
handle! {StatementHandle => EnYacHandleType_YAC_HANDLE_STMT; DbcHandle}

impl EnvHandle {
    pub fn with_utf8(self) -> Self {
        let v = EnYacCharsetCode_YAC_CHARSET_UTF8 as usize;
        unsafe {
            yacSetEnvAttr(
                self.0,
                EnYacEnvAttr_YAC_ATTR_CHARSET_CODE,
                &v as *const _ as *mut _,
                std::mem::size_of_val(&v) as _,
            );
        };
        self
    }
}
