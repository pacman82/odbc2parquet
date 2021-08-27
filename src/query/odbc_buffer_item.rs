use odbc_api::{Bit, buffers::{AnyColumnView, BufferKind, OptIt}, sys::Date};

pub trait OdbcBufferItem: Sized + Copy {
    /// E.g. [`BufferKind::I64`] for `i64`.
    const BUFFER_KIND: BufferKind;

    /// Extract the array type from an `AnyColumnView`. Will panic should the instance actually be
    /// a different variant.
    fn plain_buffer(variant: AnyColumnView<'_>) -> &[Self];
    /// Extract the typed nullable buffer from an `AnyColumnView`.  Will panic should the instance
    /// actually be a different variant.
    fn nullable_buffer(variant: AnyColumnView<'_>) -> OptIt<Self>;
}

macro_rules! impl_fixed_sized_item {
    ($t:ident, $plain:ident, $null:ident) => {  
        impl OdbcBufferItem for $t {
            const BUFFER_KIND: BufferKind = BufferKind::$plain;
        
            fn plain_buffer(variant: AnyColumnView<'_>) -> &[Self] {
                match variant {
                    AnyColumnView::$plain(vals) => vals,
                    _ => panic!("Unexepected variant in AnyColumnView."),
                }
            }
        
            fn nullable_buffer(variant: AnyColumnView<'_>) -> OptIt<Self> {
                match variant {
                    AnyColumnView::$null(vals) => vals,
                    _ => panic!("Unexepected variant in AnyColumnView."),
                }
            }
        }
    };
}

impl_fixed_sized_item!(f64, F64, NullableF64);
impl_fixed_sized_item!(f32, F32, NullableF32);
impl_fixed_sized_item!(i32, I32, NullableI32);
impl_fixed_sized_item!(i64, I64, NullableI64);
impl_fixed_sized_item!(Date, Date, NullableDate);
impl_fixed_sized_item!(Bit, Bit, NullableBit);